import { Logger } from '@config/logger.config';
import { BaileysEventMap } from 'baileys';
import { concatMap, from, Subject, Subscription } from 'rxjs';

type MessageEventName = 'messages.upsert' | 'messages.update';
type ProcessorPayload = BaileysEventMap['messages.upsert'] | BaileysEventMap['messages.update'];

type QueueItem = {
  eventName: MessageEventName;
  payload: ProcessorPayload;
  settings: any;
  resolve: () => void;
  reject: (error: unknown) => void;
};

type MountProps = {
  onEvent: (eventName: MessageEventName, payload: ProcessorPayload, settings: any) => Promise<void>;
  onFinalError?: (eventName: MessageEventName, payload: ProcessorPayload, error: unknown) => Promise<void> | void;
};

export class UnreconciledMessageUpdateError extends Error {
  constructor(
    message: string,
    public readonly metadata: Record<string, any> = {},
  ) {
    super(message);
    this.name = 'UnreconciledMessageUpdateError';
  }
}

export class BaileysMessageProcessor {
  private processorLogs = new Logger('BaileysMessageProcessor');
  private subscription?: Subscription;
  private readonly retryDelaysMs = [1000, 3000, 10000];

  protected messageSubject = new Subject<QueueItem>();

  mount({ onEvent, onFinalError }: MountProps) {
    if (this.subscription && !this.subscription.closed) {
      this.subscription.unsubscribe();
    }

    if (this.messageSubject.closed) {
      this.processorLogs.warn('MessageSubject was closed, recreating...');
      this.messageSubject = new Subject<QueueItem>();
    }

    this.subscription = this.messageSubject
      .pipe(concatMap((item) => from(this.processQueueItem(item, onEvent, onFinalError))))
      .subscribe({
        error: (error) => {
          this.processorLogs.error(`Message stream error: ${error}`);
        },
      });
  }

  public async processEvent(eventName: MessageEventName, payload: ProcessorPayload, settings: any) {
    return await new Promise<void>((resolve, reject) => {
      this.messageSubject.next({
        eventName,
        payload,
        settings,
        resolve,
        reject,
      });
    });
  }

  onDestroy() {
    this.subscription?.unsubscribe();
    this.messageSubject.complete();
  }

  private async processQueueItem(
    item: QueueItem,
    onEvent: MountProps['onEvent'],
    onFinalError?: MountProps['onFinalError'],
  ) {
    try {
      await this.executeWithRetry(item, onEvent);
      item.resolve();
    } catch (error) {
      this.processorLogs.error(
        `Error processing ${item.eventName}: ${error instanceof Error ? error.message : String(error)}`,
      );

      if (onFinalError) {
        await onFinalError(item.eventName, item.payload, error);
      }

      item.reject(error);
    }
  }

  private async executeWithRetry(item: QueueItem, onEvent: MountProps['onEvent']) {
    let genericAttempt = 0;
    let unreconciledAttempt = 0;
    const shouldRetry = true;

    while (shouldRetry) {
      try {
        await onEvent(item.eventName, item.payload, item.settings);
        return;
      } catch (error) {
        if (error instanceof UnreconciledMessageUpdateError) {
          const retryDelay = this.retryDelaysMs[unreconciledAttempt];
          if (retryDelay === undefined) {
            throw error;
          }

          unreconciledAttempt++;
          this.processorLogs.warn(
            `Retrying ${item.eventName} after unresolved reconciliation (${unreconciledAttempt}/${this.retryDelaysMs.length})`,
          );
          await this.sleep(retryDelay);
          continue;
        }

        if (genericAttempt >= 2) {
          throw error;
        }

        genericAttempt++;
        this.processorLogs.warn(
          `Retrying ${item.eventName} due to generic error (${genericAttempt}/3): ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        await this.sleep(1000);
      }
    }
  }

  private async sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}

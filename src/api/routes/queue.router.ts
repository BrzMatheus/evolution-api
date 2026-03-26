import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { DEFAULT_QUEUE_CONFIG } from '@api/integrations/queue/outbound-queue.config';
import { OutboundQueueManager } from '@api/integrations/queue/outbound-queue.manager';
import { waMonitor } from '@api/server.module';
import { RequestHandler, Router } from 'express';

import { HttpStatus } from './index.router';

export class QueueRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();

    this.router
      .get(this.routerPath('status'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: async (instance) => {
            const inst = waMonitor.waInstances[instance.instanceName];
            const queue = inst?.outboundQueue;
            if (!queue || !queue.isEnabled()) {
              return { enabled: false, message: 'Outbound queue is not enabled for this instance' };
            }
            return {
              enabled: true,
              ...queue.getMetrics(),
            };
          },
        });

        res.status(HttpStatus.OK).json(response);
      })
      .get(this.routerPath('config'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: async (instance) => {
            const inst = waMonitor.waInstances[instance.instanceName];
            const queue = inst?.outboundQueue;
            if (!queue) {
              return { enabled: false, config: DEFAULT_QUEUE_CONFIG };
            }
            return { enabled: queue.isEnabled(), config: queue.getConfig() };
          },
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('config'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: async (instance) => {
            const inst = waMonitor.waInstances[instance.instanceName] as any;
            if (!inst) {
              return { error: 'Instance not found' };
            }

            const body = req.body;

            // If queue doesn't exist yet, create it
            if (!inst.outboundQueue) {
              inst.outboundQueue = new OutboundQueueManager({
                instanceId: inst.instanceId || instance.instanceName,
                config: { ...DEFAULT_QUEUE_CONFIG, enabled: true, ...body },
                sendFn: inst.executeSend?.bind(inst),
                clientFn: () => inst.client,
                logger: inst.logger,
              });
              return { message: 'Queue created and configured', config: inst.outboundQueue.getConfig() };
            }

            inst.outboundQueue.updateConfig(body);
            return { message: 'Queue config updated', config: inst.outboundQueue.getConfig() };
          },
        });

        res.status(HttpStatus.OK).json(response);
      })
      .post(this.routerPath('flush'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: async (instance) => {
            const inst = waMonitor.waInstances[instance.instanceName];
            const queue = inst?.outboundQueue;
            if (!queue || !queue.isEnabled()) {
              return { enabled: false, message: 'Outbound queue is not enabled for this instance' };
            }
            await queue.drain();
            return { flushed: true };
          },
        });

        res.status(HttpStatus.OK).json(response);
      });
  }

  public readonly router: Router = Router();
}

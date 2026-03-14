import { CacheConf, CacheConfRedis, configService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { createClient, RedisClientType } from 'redis';

class Redis {
  private logger = new Logger('Redis');
  private client: RedisClientType = null;
  private conf: CacheConfRedis;
  private connected = false;
  private connectPromise: Promise<boolean> | null = null;

  constructor() {
    this.conf = configService.get<CacheConf>('CACHE')?.REDIS;
  }

  getConnection(): RedisClientType {
    if (!this.conf?.URI) {
      this.logger.error('redis URI not configured');
      return null;
    }

    if (this.client) {
      return this.client;
    }

    this.client = createClient({
      url: this.conf.URI,
    });

    this.client.on('connect', () => {
      this.logger.verbose('redis connecting');
    });

    this.client.on('ready', () => {
      this.logger.verbose('redis ready');
      this.connected = true;
    });

    this.client.on('error', (error) => {
      this.logger.error('redis disconnected');
      this.logger.error(error);
      this.connected = false;
    });

    this.client.on('end', () => {
      this.logger.verbose('redis connection ended');
      this.connected = false;
    });

    this.connectPromise = this.client
      .connect()
      .then(async () => {
        await this.client.ping();
        this.connected = this.client.isReady;
        return this.connected;
      })
      .catch((error) => {
        this.connected = false;
        this.logger.error('redis connect exception caught: ' + error);
        return false;
      });

    return this.client;
  }

  public isReady() {
    return !!this.client?.isReady && this.connected;
  }

  public async waitUntilReady(timeoutMs = 5000) {
    const client = this.getConnection();

    if (!client) {
      return false;
    }

    if (this.isReady()) {
      return true;
    }

    if (!this.connectPromise) {
      return false;
    }

    const timeout = new Promise<boolean>((resolve) => {
      setTimeout(() => resolve(false), timeoutMs);
    });

    return Promise.race([this.connectPromise.then(() => this.isReady()).catch(() => false), timeout]);
  }
}

export const redisClient = new Redis();

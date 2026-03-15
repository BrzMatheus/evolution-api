import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { ChatwootDto } from '@api/integrations/chatbot/chatwoot/dto/chatwoot.dto';
import {
  ChatwootHistoryAnalyzeDto,
  ChatwootHistoryContactActionDto,
  ChatwootHistoryExecuteDto,
  ChatwootHistoryReprocessDto,
} from '@api/integrations/chatbot/chatwoot/dto/chatwoot-history.dto';
import { HttpStatus } from '@api/routes/index.router';
import { chatwootController, chatwootHistoryController } from '@api/server.module';
import { chatwootSchema, instanceSchema } from '@validate/validate.schema';
import { RequestHandler, Router } from 'express';

import {
  chatwootHistoryAnalyzeSchema,
  chatwootHistoryContactActionSchema,
  chatwootHistoryExecuteSchema,
  chatwootHistoryReprocessSchema,
} from '../validate/chatwoot-history.schema';

export class ChatwootRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();
    this.router
      .post(this.routerPath('set'), ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootDto>({
          request: req,
          schema: chatwootSchema,
          ClassRef: ChatwootDto,
          execute: (instance, data) => chatwootController.createChatwoot(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get(this.routerPath('find'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance) => chatwootController.findChatwoot(instance),
        });

        res.status(HttpStatus.OK).json(response);
      })
      .get('/inbox/status/:instanceName', ...guards, async (req, res) => {
        const instance = req.params as unknown as InstanceDto;
        const response = await chatwootHistoryController.getInboxStatus(instance);

        res.status(HttpStatus.OK).json(response);
      })
      .post('/history/analyze/:instanceName', ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootHistoryAnalyzeDto>({
          request: req,
          schema: chatwootHistoryAnalyzeSchema,
          ClassRef: ChatwootHistoryAnalyzeDto,
          execute: (instance, data) => chatwootHistoryController.analyze(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .post('/history/execute/:instanceName', ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootHistoryExecuteDto>({
          request: req,
          schema: chatwootHistoryExecuteSchema,
          ClassRef: ChatwootHistoryExecuteDto,
          execute: (instance, data) => chatwootHistoryController.execute(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get('/history/jobs/:instanceName', ...guards, async (req, res) => {
        const instance = req.params as unknown as InstanceDto;
        const response = await chatwootHistoryController.listJobs(instance);

        res.status(HttpStatus.OK).json(response);
      })
      .get('/history/job/:instanceName', ...guards, async (req, res) => {
        const instance = req.params as unknown as InstanceDto;
        const { jobId } = req.query as { jobId: string };
        if (!jobId) {
          return res.status(HttpStatus.BAD_REQUEST).json({ error: 'jobId is a required query parameter' });
        }

        const response = await chatwootHistoryController.getJob(instance, jobId);

        res.status(HttpStatus.OK).json(response);
      })
      .get('/history/conflicts/:instanceName', ...guards, async (req, res) => {
        const instance = req.params as unknown as InstanceDto;
        const response = await chatwootHistoryController.listConflicts(instance);

        res.status(HttpStatus.OK).json(response);
      })
      .post('/history/reprocess/:instanceName', ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootHistoryReprocessDto>({
          request: req,
          schema: chatwootHistoryReprocessSchema,
          ClassRef: ChatwootHistoryReprocessDto,
          execute: (instance, data) => chatwootHistoryController.reprocess(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .post('/history/contact-action/:instanceName', ...guards, async (req, res) => {
        const response = await this.dataValidate<ChatwootHistoryContactActionDto>({
          request: req,
          schema: chatwootHistoryContactActionSchema,
          ClassRef: ChatwootHistoryContactActionDto,
          execute: (instance, data) => chatwootHistoryController.contactAction(instance, data),
        });

        res.status(HttpStatus.CREATED).json(response);
      })
      .get('/history/export/:instanceName', ...guards, async (req, res) => {
        const instance = req.params as unknown as InstanceDto;
        const { jobId } = req.query as { jobId: string };
        if (!jobId) {
          return res.status(HttpStatus.BAD_REQUEST).json({ error: 'jobId is a required query parameter' });
        }

        const response = await chatwootHistoryController.exportCsv(instance, jobId);
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="chatwoot-history-${jobId}.csv"`);
        res.status(HttpStatus.OK).send(response);
      })
      .post(this.routerPath('webhook'), async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: instanceSchema,
          ClassRef: InstanceDto,
          execute: (instance, data) => chatwootController.receiveWebhook(instance, data),
        });

        res.status(HttpStatus.OK).json(response);
      });
  }

  public readonly router: Router = Router();
}

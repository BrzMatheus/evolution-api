import { prismaRepository } from '@api/server.module';
import { configService, Database } from '@config/env.config';
import { Logger } from '@config/logger.config';
import dayjs from 'dayjs';

const logger = new Logger('OnWhatsappCache');

function getAvailableNumbers(remoteJid: string) {
  if (remoteJid.startsWith('+')) {
    remoteJid = remoteJid.slice(1);
  }

  const [number, domain] = remoteJid.split('@');
  const numbersAvailable = new Set<string>();

  if (!number || !domain) {
    return remoteJid ? [remoteJid] : [];
  }

  if (domain === 'lid' || domain === 'g.us') {
    return [remoteJid];
  }

  numbersAvailable.add(number);

  // For Brazil, only add the mobile 9-digit variant when it is missing.
  if (number.startsWith('55') && number.length === 12) {
    numbersAvailable.add(`${number.slice(0, 4)}9${number.slice(4)}`);
  }

  // Ref: https://faq.whatsapp.com/1294841057948784
  if ((number.startsWith('52') || number.startsWith('54')) && number.length === 12) {
    const prefix = number.startsWith('52') ? '1' : '9';
    numbersAvailable.add(`${number.slice(0, 2)}${prefix}${number.slice(2)}`);
  }

  return [...numbersAvailable].map((availableNumber) => `${availableNumber}@${domain}`);
}

interface ISaveOnWhatsappCacheParams {
  remoteJid: string;
  remoteJidAlt?: string;
  lid?: 'lid' | undefined;
}

function normalizeJid(jid: string | null | undefined): string | null {
  if (!jid) return null;
  return jid.startsWith('+') ? jid.slice(1) : jid;
}

export async function saveOnWhatsappCache(data: ISaveOnWhatsappCacheParams[]) {
  if (!configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP) {
    return;
  }

  const processingPromises = data.map(async (item) => {
    try {
      const remoteJid = normalizeJid(item.remoteJid);
      if (!remoteJid) {
        logger.warn('[saveOnWhatsappCache] Item skipped, missing remoteJid.');
        return;
      }

      const altJidNormalized = normalizeJid(item.remoteJidAlt);
      const lidAltJid = altJidNormalized && altJidNormalized.includes('@lid') ? altJidNormalized : null;

      const baseJids = [remoteJid];
      if (lidAltJid) {
        baseJids.push(lidAltJid);
      }

      const expandedJids = baseJids.flatMap((jid) => getAvailableNumbers(jid));

      const existingRecord = await prismaRepository.isOnWhatsapp.findFirst({
        where: {
          OR: [...expandedJids.map((jid) => ({ jidOptions: { contains: jid } })), { remoteJid }],
        },
      });

      logger.verbose(
        `[saveOnWhatsappCache] Register exists for [${expandedJids.join(',')}]? => ${existingRecord ? existingRecord.remoteJid : 'Not found'}`,
      );

      const finalJidOptions = new Set(expandedJids);

      if (lidAltJid) {
        finalJidOptions.add(lidAltJid);
      }

      if (existingRecord?.jidOptions) {
        existingRecord.jidOptions.split(',').forEach((jid) => finalJidOptions.add(jid));
      }

      const sortedJidOptions = [...finalJidOptions].sort();
      const newJidOptionsString = sortedJidOptions.join(',');
      const newLid = item.lid === 'lid' || item.remoteJid?.includes('@lid') ? 'lid' : null;

      const dataPayload = {
        remoteJid,
        jidOptions: newJidOptionsString,
        lid: newLid,
      };

      if (existingRecord) {
        const existingJidOptionsString = existingRecord.jidOptions
          ? existingRecord.jidOptions.split(',').sort().join(',')
          : '';

        const isDataSame =
          existingRecord.remoteJid === dataPayload.remoteJid &&
          existingJidOptionsString === dataPayload.jidOptions &&
          existingRecord.lid === dataPayload.lid;

        if (isDataSame) {
          logger.verbose(`[saveOnWhatsappCache] Data for ${remoteJid} is already up-to-date. Skipping update.`);
          return;
        }

        logger.verbose(
          `[saveOnWhatsappCache] Register exists, updating: remoteJid=${remoteJid}, jidOptions=${dataPayload.jidOptions}, lid=${dataPayload.lid}`,
        );
        await prismaRepository.isOnWhatsapp.update({
          where: { id: existingRecord.id },
          data: dataPayload,
        });
      } else {
        logger.verbose(
          `[saveOnWhatsappCache] Register does not exist, creating: remoteJid=${remoteJid}, jidOptions=${dataPayload.jidOptions}, lid=${dataPayload.lid}`,
        );
        await prismaRepository.isOnWhatsapp.create({
          data: dataPayload,
        });
      }
    } catch (e) {
      logger.error(`[saveOnWhatsappCache] Error processing item for ${item.remoteJid}: `);
      logger.error(e);
    }
  });

  await Promise.allSettled(processingPromises);
}

export async function getOnWhatsappCache(remoteJids: string[]) {
  let results: {
    remoteJid: string;
    number: string;
    jidOptions: string[];
    lid?: string;
  }[] = [];

  if (configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP) {
    const remoteJidsWithoutPlus = remoteJids.map((remoteJid) => getAvailableNumbers(remoteJid)).flat();

    const onWhatsappCache = await prismaRepository.isOnWhatsapp.findMany({
      where: {
        OR: remoteJidsWithoutPlus.map((remoteJid) => ({ jidOptions: { contains: remoteJid } })),
        updatedAt: {
          gte: dayjs().subtract(configService.get<Database>('DATABASE').SAVE_DATA.IS_ON_WHATSAPP_DAYS, 'days').toDate(),
        },
      },
    });

    results = onWhatsappCache.map((item) => ({
      remoteJid: item.remoteJid,
      number: item.remoteJid.split('@')[0],
      jidOptions: item.jidOptions.split(','),
      lid: item.lid,
    }));
  }

  return results;
}

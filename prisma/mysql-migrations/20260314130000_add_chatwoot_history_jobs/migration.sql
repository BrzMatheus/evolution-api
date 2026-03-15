-- CreateTable
CREATE TABLE `ChatwootHistoryJob` (
  `id` VARCHAR(191) NOT NULL,
  `scopeType` ENUM('single', 'selected', 'eligibleAll') NOT NULL,
  `mode` ENUM('dryRun', 'importDirect', 'rebuild') NOT NULL,
  `jobStatus` ENUM('pending', 'running', 'completed', 'failed') NOT NULL DEFAULT 'pending',
  `summary` JSON NULL,
  `filters` JSON NULL,
  `errorMessage` TEXT NULL,
  `startedAt` TIMESTAMP NULL,
  `finishedAt` TIMESTAMP NULL,
  `createdAt` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  `updatedAt` TIMESTAMP NOT NULL,
  `instanceId` VARCHAR(191) NOT NULL,

  PRIMARY KEY (`id`),
  INDEX `ChatwootHistoryJob_instanceId_idx`(`instanceId`),
  INDEX `ChatwootHistoryJob_instanceId_jobStatus_idx`(`instanceId`, `jobStatus`),
  INDEX `ChatwootHistoryJob_instanceId_createdAt_idx`(`instanceId`, `createdAt`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `ChatwootHistoryJobContact` (
  `id` VARCHAR(191) NOT NULL,
  `remoteJid` VARCHAR(100) NOT NULL,
  `canonicalJid` VARCHAR(100) NULL,
  `pushName` VARCHAR(255) NULL,
  `classification` ENUM('eligible', 'needs_review', 'lid_alias', 'requires_rebuild', 'ignored') NOT NULL,
  `suggestedAction` ENUM('import_direct', 'create_rebuild', 'open_chatwoot', 'ignore') NOT NULL,
  `selectedAction` ENUM('import_direct', 'create_rebuild', 'open_chatwoot', 'ignore') NULL,
  `executionStatus` ENUM('pending', 'completed', 'failed', 'skipped') NOT NULL DEFAULT 'pending',
  `hasLidAlias` BOOLEAN NOT NULL DEFAULT false,
  `evolutionMessageCount` INTEGER NOT NULL DEFAULT 0,
  `chatwootMessageCount` INTEGER NOT NULL DEFAULT 0,
  `overlapCount` INTEGER NOT NULL DEFAULT 0,
  `chatwootContactId` INTEGER NULL,
  `existingConversationId` INTEGER NULL,
  `rebuiltConversationId` INTEGER NULL,
  `report` JSON NULL,
  `createdAt` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  `updatedAt` TIMESTAMP NOT NULL,
  `jobId` VARCHAR(191) NOT NULL,
  `instanceId` VARCHAR(191) NOT NULL,

  PRIMARY KEY (`id`),
  UNIQUE INDEX `ChatwootHistoryJobContact_jobId_remoteJid_key`(`jobId`, `remoteJid`),
  INDEX `ChatwootHistoryJobContact_jobId_idx`(`jobId`),
  INDEX `ChatwootHistoryJobContact_instanceId_idx`(`instanceId`),
  INDEX `ChatwootHistoryJobContact_instanceId_classification_idx`(`instanceId`, `classification`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `ChatwootHistoryJob`
  ADD CONSTRAINT `ChatwootHistoryJob_instanceId_fkey`
  FOREIGN KEY (`instanceId`) REFERENCES `Instance`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `ChatwootHistoryJobContact`
  ADD CONSTRAINT `ChatwootHistoryJobContact_jobId_fkey`
  FOREIGN KEY (`jobId`) REFERENCES `ChatwootHistoryJob`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `ChatwootHistoryJobContact`
  ADD CONSTRAINT `ChatwootHistoryJobContact_instanceId_fkey`
  FOREIGN KEY (`instanceId`) REFERENCES `Instance`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

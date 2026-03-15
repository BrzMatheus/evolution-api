ALTER TABLE `ChatwootHistoryJob`
MODIFY COLUMN `jobStatus` ENUM('pending', 'analyzing', 'awaiting_execution', 'running', 'completed', 'failed', 'partial') NOT NULL DEFAULT 'pending',
ADD COLUMN `report` JSON NULL AFTER `summary`;

ALTER TABLE `ChatwootHistoryJobContact`
ADD COLUMN `phoneJid` VARCHAR(100) NULL AFTER `canonicalJid`,
ADD COLUMN `lidJid` VARCHAR(100) NULL AFTER `phoneJid`,
ADD COLUMN `canonicalIdentityType` ENUM('s_whatsapp_net', 'unresolved') NOT NULL DEFAULT 'unresolved' AFTER `lidJid`,
ADD COLUMN `identityResolutionStatus` ENUM('resolved', 'alias_only', 'ambiguous') NOT NULL DEFAULT 'ambiguous' AFTER `canonicalIdentityType`,
ADD COLUMN `isSafeDirectImport` BOOLEAN NOT NULL DEFAULT FALSE AFTER `hasLidAlias`,
ADD COLUMN `selectedConversationId` INT NULL AFTER `existingConversationId`,
ADD COLUMN `unsafeReasons` JSON NULL AFTER `rebuiltConversationId`,
ADD COLUMN `candidateConversationIds` JSON NULL AFTER `unsafeReasons`;

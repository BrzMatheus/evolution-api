ALTER TABLE `Message`
ADD COLUMN `keyId` VARCHAR(100) NULL,
ADD COLUMN `fromMe` BOOLEAN NULL,
ADD COLUMN `canonicalJid` VARCHAR(100) NULL,
ADD COLUMN `phoneJid` VARCHAR(100) NULL,
ADD COLUMN `lidJid` VARCHAR(100) NULL;

ALTER TABLE `MessageUpdate`
ADD COLUMN `canonicalJid` VARCHAR(100) NULL,
ADD COLUMN `phoneJid` VARCHAR(100) NULL,
ADD COLUMN `lidJid` VARCHAR(100) NULL;

CREATE INDEX `Message_instanceId_keyId_idx` ON `Message`(`instanceId`, `keyId`);
CREATE INDEX `Message_instanceId_canonicalJid_fromMe_messageTimestamp_idx`
ON `Message`(`instanceId`, `canonicalJid`, `fromMe`, `messageTimestamp`);
CREATE INDEX `MessageUpdate_instanceId_keyId_idx` ON `MessageUpdate`(`instanceId`, `keyId`);
CREATE INDEX `MessageUpdate_instanceId_canonicalJid_fromMe_idx`
ON `MessageUpdate`(`instanceId`, `canonicalJid`, `fromMe`);

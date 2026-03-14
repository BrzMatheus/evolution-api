ALTER TABLE "Message"
ADD COLUMN "keyId" VARCHAR(100),
ADD COLUMN "fromMe" BOOLEAN,
ADD COLUMN "canonicalJid" VARCHAR(100),
ADD COLUMN "phoneJid" VARCHAR(100),
ADD COLUMN "lidJid" VARCHAR(100);

ALTER TABLE "MessageUpdate"
ADD COLUMN "canonicalJid" VARCHAR(100),
ADD COLUMN "phoneJid" VARCHAR(100),
ADD COLUMN "lidJid" VARCHAR(100);

CREATE INDEX "Message_instanceId_keyId_idx" ON "Message"("instanceId", "keyId");
CREATE INDEX "Message_instanceId_canonicalJid_fromMe_messageTimestamp_idx"
ON "Message"("instanceId", "canonicalJid", "fromMe", "messageTimestamp");
CREATE INDEX "MessageUpdate_instanceId_keyId_idx" ON "MessageUpdate"("instanceId", "keyId");
CREATE INDEX "MessageUpdate_instanceId_canonicalJid_fromMe_idx"
ON "MessageUpdate"("instanceId", "canonicalJid", "fromMe");

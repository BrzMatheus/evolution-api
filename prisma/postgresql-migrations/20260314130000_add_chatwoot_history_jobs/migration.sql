-- CreateEnum
CREATE TYPE "ChatwootHistoryScopeType" AS ENUM ('single', 'selected', 'eligibleAll');

-- CreateEnum
CREATE TYPE "ChatwootHistoryJobStatus" AS ENUM ('pending', 'running', 'completed', 'failed');

-- CreateEnum
CREATE TYPE "ChatwootHistoryJobMode" AS ENUM ('dryRun', 'importDirect', 'rebuild');

-- CreateEnum
CREATE TYPE "ChatwootHistoryClassification" AS ENUM (
  'eligible',
  'needs_review',
  'lid_alias',
  'requires_rebuild',
  'ignored'
);

-- CreateEnum
CREATE TYPE "ChatwootHistorySuggestedAction" AS ENUM (
  'import_direct',
  'create_rebuild',
  'open_chatwoot',
  'ignore'
);

-- CreateEnum
CREATE TYPE "ChatwootHistoryExecutionStatus" AS ENUM ('pending', 'completed', 'failed', 'skipped');

-- CreateTable
CREATE TABLE "ChatwootHistoryJob" (
  "id" TEXT NOT NULL,
  "scopeType" "ChatwootHistoryScopeType" NOT NULL,
  "mode" "ChatwootHistoryJobMode" NOT NULL,
  "jobStatus" "ChatwootHistoryJobStatus" NOT NULL DEFAULT 'pending',
  "summary" JSONB,
  "filters" JSONB,
  "errorMessage" TEXT,
  "startedAt" TIMESTAMP(3),
  "finishedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  "instanceId" TEXT NOT NULL,

  CONSTRAINT "ChatwootHistoryJob_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ChatwootHistoryJobContact" (
  "id" TEXT NOT NULL,
  "remoteJid" VARCHAR(100) NOT NULL,
  "canonicalJid" VARCHAR(100),
  "pushName" VARCHAR(255),
  "classification" "ChatwootHistoryClassification" NOT NULL,
  "suggestedAction" "ChatwootHistorySuggestedAction" NOT NULL,
  "selectedAction" "ChatwootHistorySuggestedAction",
  "executionStatus" "ChatwootHistoryExecutionStatus" NOT NULL DEFAULT 'pending',
  "hasLidAlias" BOOLEAN NOT NULL DEFAULT false,
  "evolutionMessageCount" INTEGER NOT NULL DEFAULT 0,
  "chatwootMessageCount" INTEGER NOT NULL DEFAULT 0,
  "overlapCount" INTEGER NOT NULL DEFAULT 0,
  "chatwootContactId" INTEGER,
  "existingConversationId" INTEGER,
  "rebuiltConversationId" INTEGER,
  "report" JSONB,
  "createdAt" TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  "jobId" TEXT NOT NULL,
  "instanceId" TEXT NOT NULL,

  CONSTRAINT "ChatwootHistoryJobContact_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "ChatwootHistoryJob_instanceId_idx" ON "ChatwootHistoryJob"("instanceId");

-- CreateIndex
CREATE INDEX "ChatwootHistoryJob_instanceId_jobStatus_idx" ON "ChatwootHistoryJob"("instanceId", "jobStatus");

-- CreateIndex
CREATE INDEX "ChatwootHistoryJob_instanceId_createdAt_idx" ON "ChatwootHistoryJob"("instanceId", "createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "ChatwootHistoryJobContact_jobId_remoteJid_key" ON "ChatwootHistoryJobContact"("jobId", "remoteJid");

-- CreateIndex
CREATE INDEX "ChatwootHistoryJobContact_jobId_idx" ON "ChatwootHistoryJobContact"("jobId");

-- CreateIndex
CREATE INDEX "ChatwootHistoryJobContact_instanceId_idx" ON "ChatwootHistoryJobContact"("instanceId");

-- CreateIndex
CREATE INDEX "ChatwootHistoryJobContact_instanceId_classification_idx"
  ON "ChatwootHistoryJobContact"("instanceId", "classification");

-- AddForeignKey
ALTER TABLE "ChatwootHistoryJob"
  ADD CONSTRAINT "ChatwootHistoryJob_instanceId_fkey"
  FOREIGN KEY ("instanceId") REFERENCES "Instance"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ChatwootHistoryJobContact"
  ADD CONSTRAINT "ChatwootHistoryJobContact_jobId_fkey"
  FOREIGN KEY ("jobId") REFERENCES "ChatwootHistoryJob"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ChatwootHistoryJobContact"
  ADD CONSTRAINT "ChatwootHistoryJobContact_instanceId_fkey"
  FOREIGN KEY ("instanceId") REFERENCES "Instance"("id") ON DELETE CASCADE ON UPDATE CASCADE;

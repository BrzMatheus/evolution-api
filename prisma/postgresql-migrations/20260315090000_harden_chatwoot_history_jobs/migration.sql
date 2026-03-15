ALTER TYPE "ChatwootHistoryJobStatus" ADD VALUE IF NOT EXISTS 'analyzing';
ALTER TYPE "ChatwootHistoryJobStatus" ADD VALUE IF NOT EXISTS 'awaiting_execution';
ALTER TYPE "ChatwootHistoryJobStatus" ADD VALUE IF NOT EXISTS 'partial';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ChatwootHistoryCanonicalIdentityType') THEN
    CREATE TYPE "ChatwootHistoryCanonicalIdentityType" AS ENUM ('s_whatsapp_net', 'unresolved');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ChatwootHistoryIdentityResolutionStatus') THEN
    CREATE TYPE "ChatwootHistoryIdentityResolutionStatus" AS ENUM ('resolved', 'alias_only', 'ambiguous');
  END IF;
END $$;

ALTER TABLE "ChatwootHistoryJob"
ADD COLUMN IF NOT EXISTS "report" JSONB;

ALTER TABLE "ChatwootHistoryJobContact"
ADD COLUMN IF NOT EXISTS "phoneJid" VARCHAR(100),
ADD COLUMN IF NOT EXISTS "lidJid" VARCHAR(100),
ADD COLUMN IF NOT EXISTS "canonicalIdentityType" "ChatwootHistoryCanonicalIdentityType" NOT NULL DEFAULT 'unresolved',
ADD COLUMN IF NOT EXISTS "identityResolutionStatus" "ChatwootHistoryIdentityResolutionStatus" NOT NULL DEFAULT 'ambiguous',
ADD COLUMN IF NOT EXISTS "isSafeDirectImport" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS "selectedConversationId" INTEGER,
ADD COLUMN IF NOT EXISTS "unsafeReasons" JSONB,
ADD COLUMN IF NOT EXISTS "candidateConversationIds" JSONB;

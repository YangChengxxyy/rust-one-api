-- Phase 3: LB priority tiering (axonhub channel priority ordering policy).

ALTER TABLE channels ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_channels_priority ON channels (priority);

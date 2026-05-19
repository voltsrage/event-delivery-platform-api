/*
  Warnings:

  - Added the required column `secret_raw` to the `subscriptions` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE subscriptions
  ADD COLUMN secret_raw TEXT NOT NULL DEFAULT '';

-- Remove the temporary default so future rows require an explicit value.
ALTER TABLE subscriptions
  ALTER COLUMN secret_raw DROP DEFAULT;

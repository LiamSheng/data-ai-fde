#!/usr/bin/env bash
# Builds the site and uploads it to the web bucket behind ai.linzi.info (KAN-28).
# Needs gcloud signed in with write access to the bucket, and web/.env with PUBLIC_GOOGLE_MAPS_API_KEY.
set -euo pipefail

BUCKET="${WEB_BUCKET:-ai-linzi-info-web-26}"
cd "$(dirname "$0")/.."

npm run build

# 1. Hashed assets first, so a new page never points at a file that is not there yet.
#    Their names change with their content, so browsers and the CDN may keep them for a year.
gcloud storage rsync dist/_astro "gs://${BUCKET}/_astro" --recursive \
  --cache-control="public, max-age=31536000, immutable"

# 2. Everything else (HTML, favicon): revalidated on every visit, so a release shows at once.
gcloud storage rsync dist "gs://${BUCKET}" --recursive --exclude="^_astro/" \
  --cache-control="no-cache"

# Nothing is deleted: old hashed assets stay (small, and pages cached before the release may still ask for them).
echo "Published to gs://${BUCKET}"

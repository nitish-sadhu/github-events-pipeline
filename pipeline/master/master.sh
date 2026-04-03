#!/bin/zsh

set -e

export PATH="/Users/krishnasadhu/google-cloud-sdk/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export GOOGLE_CLOUD_PROJECT="github-events-analytics"

which uv
which python3
which gcloud

cd /Users/krishnasadhu/github-events-analytics

CURR_DATE=$(date -v-2d +"%Y-%m-%d")
PREV_HOUR=$(date -v-1H +"%H")

# /Users/krishnasadhu/github-events-analytics/.venv/bin/python
# extract_to_gcs

/opt/homebrew/bin/uv run /Users/krishnasadhu/github-events-analytics/.venv/bin/python -m pipeline.extract.extract_to_gcs --date $CURR_DATE --hour $PREV_HOUR

/opt/homebrew/bin/uv run /Users/krishnasadhu/github-events-analytics/.venv/bin/python -m pipeline.transform.stage_1.convert_to_parquet --date $CURR_DATE --hour $PREV_HOUR

/opt/homebrew/bin/uv run /Users/krishnasadhu/github-events-analytics/.venv/bin/python -m pipeline.transform.stage_2.create_ext_table

cd /Users/krishnasadhu/github-events-analytics/pipeline/transform/stage_3/
source /Users/krishnasadhu/github-events-analytics/pipeline/transform/stage_3/env/bin/activate
cd /Users/krishnasadhu/github-events-analytics/pipeline/transform/stage_3/stage_3_transform
dbt run
dbt test
deactivate

CLEAN_UP_DATE=$(date -v-30d +"%Y-%m-%d")

/opt/homebrew/bin/uv run /Users/krishnasadhu/github-events-analytics/.venv/bin/python -m pipeline.clean_up.clean_up_gcs --date $CLEAN_UP_DATE --hour $PREV_HOUR

echo "___DELETED___: /$CLEAN_UP_DATE/$PREV_HOUR.json.gz"

echo "___RUN_SUCCESSFUL___: date: $CURR_DATE, hour: $PREV_DATE"
#!/bin/bash

# The purpose of this script is to run queries to provide static statistics by:
#   * test_geography_timePeriod - for example: `ndt_us_aiannh_month`
#                                     - NDT test results
#                                     - US American Indian, Alaska Native Corps, & Hawaiian Homelands
#                                     - aggregated by month
#
# This script will run the queries and save the results in:
#   `measurement-lab.mlab_statistics.<table name>`

set -eux

PROJECT="measurement-lab"
USERNAME="critzo"

declare -a query_jobs=("ndt_us_states_month"   \
                      "ndt_us_counties_month" \
                      "ndt_us_zipcode_month"   \
                      "ndt_us_116th_congress_month" \
                      #"ndt_us_aiannh_month"  \
  )

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  QUALIFIED_TABLE="${PROJECT}:mlab_statistics.${RESULT_NAME}"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.

  JOB_ID=$(bq --nosync --project_id "${PROJECT}" query \
    --allow_large_results --destination_table "${QUALIFIED_TABLE}" \
    --replace --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY}")")

  JOB_ID="${JOB_ID#Successfully started query }"

  until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
  do
    sleep 30
  done

done

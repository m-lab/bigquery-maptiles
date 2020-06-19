#!/bin/bash

# The purpose of this script is to run queries to provide static statistics by:
#   * test_geography_timePeriod - for example: `ndt_us_aiannh_month`
#                                     - NDT test results
#                                     - US American Indian, Alaska Native Corps, & Hawaiian Homelands
#                                     - aggregated by month
#
# This script will run the queries and save the results in:
#   `measurement-lab.mlab_statistics.<table name>`
#
# Use this script for jobs that do not result in maptiles, only statistics.
#
set -eux

PROJECT="measurement-lab"
USERNAME="critzo"

declare -a query_jobs=("continent_country_region_maxDL_histogram"
)

d=2020-01-01

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  QUALIFIED_TABLE="${PROJECT}:mlab_statistics.${RESULT_NAME}"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.

  while [ "$d" != 2020-06-17 ]; do
    JOB_ID=$(bq --nosync --project_id "${PROJECT}" query \
      --parameter=day::$d --allow_large_results --destination_table "${QUALIFIED_TABLE}" \
      --append_table --use_legacy_sql=false --max_rows=4000000 \
      "$(cat "queries/${QUERY}")")

    JOB_ID="${JOB_ID#Successfully started query }"

    until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
    do
      sleep 30
    done

    d=$(date -I -d "$d + 1 day")
  done


done

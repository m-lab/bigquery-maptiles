#!/bin/bash

set -eux

PROJECT="${1:?Please provide a GCP project for tile upload}"
USERNAME="critzo"
TABLE="${USERNAME}.temp"
QUALIFIED_TABLE="${PROJECT}:${TABLE}"

declare -a query_jobs=("us_counties")

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  SCHEMA="${RESULT_NAME}.json"
  QUERY="${RESULT_NAME}.sql"
  GCS_STORAGE="${RESULT_NAME}_temp"
  PUB_LOC="maptiles.mlab-sandbox.measurementlab.net/${RESULT_NAME}"

  # TODO: replace use of bq with a custom Go binary that runs the query and
  # outputs the right format. Also open an HTTP port for Cloud Run.

  # Make a temporary table using a schema definition based on the
  # expected query output fields.
  bq mk --table --expiration 3600 --description "temp table to store intermediate results before automated export" \
    "${QUALIFIED_TABLE}" "schemas/${SCHEMA}"

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

  # create a temprary GCS Storage Bucket
  gsutil mb gs://${GCS_STORAGE}

  # Generate CSV files; expected to include geometry info in WKT format.
  bq extract --destination_format CSV "${QUALIFIED_TABLE}" \
      gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv

  # Fetch the CSV files that were just exported.
  gsutil -m cp gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv ./

  # ogr2ogr requires a schema file to know which csv column represents
  # the geometry. We pass all filenames to the inference script, but
  # it only reads the first one, since the schema should be consistent
  # for all of them.
  scripts/infer_csvt_schema.sh ${RESULT_NAME}_*.csv > schema.csvt

  # Use xargs to convert all the csv files to geojson individually, in
  # parallel. We will aggregate them in the next step.  See csv_to_geojson
  # script for ogr2ogr args.
  echo csv/${RESULT_NAME}_*.csv | xargs -n1 -P4 scripts/csv_to_geojson.sh 

  # Let tippecanoe read all the geojson files into one layer.
  tippecanoe -e ./maptiles/${RESULT_NAME} -f -l ${RESULT_NAME} *.geojson -z6 \
      --simplification=10 \
      --detect-shared-borders \
      --coalesce-densest-as-needed \
      --no-tile-compression

  # Upload to cloud storage publishing location
  gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
    cp -r ./maptiles/${RESULT_NAME}/* gs://${PUB_LOC}/

  gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
    cp -r *.csv gs://${PUB_LOC}/csv/

  gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
    cp -r ./templates/us_counties.html gs://${PUB_LOC}/index.html

  # Final cleanup

  # Cleanup the files on GCS because we don't need them there anymore.
  gsutil rm -r gs://${GCS_STORAGE}
  bq rm -f ${QUALIFIED_TABLE}

  # Cleanup locally generated files
  rm *.csvt *.geojson csv/* maptiles/*

  # NOTE: if the html and tiles are served from different domains we'll need to
  # apply a CORS policy to GCS.

done

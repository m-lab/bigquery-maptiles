#!/bin/bash

set -eux

PROJECT="${1:?Please provide a GCP project for tile upload}"
TABLE="georgia.nc_counties"
QUALIFIED_TABLE="${PROJECT}:${TABLE}"
QUERIES="${QUERIES:-/maptiles}"
SCRIPTS="${SCRIPTS:-/maptiles}"
SCHEMAS="${SCHEMAS:-/maptiles}"

# TODO: replace use of bq with a custom Go binary that runs the query and
# outputs the right format. Also open an HTTP port for Cloud Run.

# Make a temporary table using a schema definition based on the expected query output fields 
bq mk --table --expiration 3600 --description "temp table for bq2maptiles process" \
	"${QUALIFIED_TABLE}" "${SCHEMAS}"/nc_counties_spjoin.json

# Run bq query with generous row limit. Write results to temp table created above.
# By default, bq fetches the query results to display in the shell, consuming a lot of memory.
# Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
# until the table is populated.
JOB_ID=$(bq --nosync --project_id "${PROJECT}" query \
	--allow_large_results --destination_table "${QUALIFIED_TABLE}" \
    --replace --use_legacy_sql=false --max_rows=4000000 "$(cat "${QUERIES}/query-bqsj.sql")")
JOB_ID="${JOB_ID#Successfully started query }"

until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
do
  sleep 30
done 

# Generate CSV files; expected to include geometry info in WKT format.
bq extract --destination_format CSV "${QUALIFIED_TABLE}" \
    gs://bigquery-maptiles-mlab-sandbox/csv/nc_counties_*.csv

# Fetch the CSV files that were just exported.
gsutil -m cp gs://bigquery-maptiles-mlab-sandbox/csv/nc_counties_*.csv ./

#Cleanup the files on GCS because we don't need them there anymore.
# gsutil rm gs://bigquery-maptiles-mlab-sandbox/csv/nc_counties_*

# ogr2ogr requires a schema file to know which csv column represents
# the geometry. We pass all filenames to the inference script, but
# it only reads the first one, since the schema should be consistent
# for all of them.
$SCRIPTS/infer_csvt_schema.sh nc_counties_*.csv > schema.csvt

# Use xargs to convert all the csv files to geojson individually, in
# parallel. We will aggregate them in the next step.  See csv_to_geojson
# script for ogr2ogr args.
echo nc_counties_*.csv | xargs -n1 -P4 $SCRIPTS/csv_to_geojson.sh 

# Let tippecanoe read all the geojson files into one layer.
tippecanoe -e /maptiles/nc_counties -f -l nc_counties *.geojson -z6 \
    --simplification=10 \
    --detect-shared-borders \
    --coalesce-densest-as-needed \
    --no-tile-compression

#upload to cloud storage box
gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
  cp -r /maptiles/nc_counties/* gs://bigquery-maptiles-${PROJECT}/maptiles/

gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
  cp -r /maptiles/example.html gs://bigquery-maptiles-${PROJECT}/example.html

# NOTE: if the html and tiles are served from different domains we'll need to
# apply a CORS policy to GCS.

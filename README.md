# bigquery-maptiles

The scripts in this repository are used to take BigQuery queries of NDT data from M-Lab, aggregated it by specific geographies and time rangs, and use Google Cloud resources to generate protobuf maptile sets, along with the CSV source data. By regularly extracting and providing static aggregations and resources like maptiles, we hope to lower the bar for audiences interested in using M-Lab data in their analyses or applications.

## How M-Lab Generates Maptiles

M-Lab uses a very well resourced VM to regularly run `scripts/generate_maptiles.sh`, so that we can automate a scheduled job, and continue with other work on a general workstation. The script requires a Google Cloud Project with billing enabled, use of the BigQuery API in that project, as well as a Google Cloud Storage bucket. The script can be run directly from the repository, and support for building within a Docker container is possible as well.

The basic command to generate tilesets and CSV source data is:

* `scripts/generate_maptiles.sh <gcp-project-name>`

In our case, we run this from a very well resourced VM called `maptiles-builder` in our "sandbox" project, `mlab-sandbox`:

* `scripts/generate_maptiles.sh mlab-sandbox`

The script does the following:

* For each query defined in `queries/`, `generate_maptiles.sh`:
  * runs the query, saving the results in a temporary BigQuery table in your GCP project
  * creates a GCS bucket and extracts the query results from the temparorary BigQuery table as CSV files, sharding the results as needed depending on the data size
  * fetches the extracted CSV files to the VM for further processing
  * removes the temporary GCS bucket and BigQuery table
  * processes the CSV files to prepare for generating protobuf formatted maptiles
    * `scripts/infer_csvt_schema.sh` uses the program `ogr2ogr` to generate a schema file
    * `scripts/csv_to_geojson.sh` processes the CSV files, producing geojson files that will be used to build protobuf maptiles
  * uses `tippecanoe` to generate maptiles
  * uploads the resulting maptiles and CSV files to a GCS bucket for publication and (optionally) sharing
  * removes local copies of the CSVs and maptiles to save storage for the next run

## Customizing `generate_maptiles.sh` for non-M-Lab use

Open `scripts/generate_maptiles.sh` in your preferred editor. At the top of the file, change these variables to match your situation:

```
PROJECT="${1:?Please provide a GCP project for tile upload}"
USERNAME="critzo"
TABLE="${USERNAME}.temp"
QUALIFIED_TABLE="${PROJECT}:${TABLE}"
PUB_LOC="maptiles.mlab-sandbox.measurementlab.net"

declare -a query_jobs=("us_state" \
                    #  "us_counties" \
                    #  "us_116th_congress" \
                    #  "us_zipcode" \
                      "us_aiannh"
  )
```

**USERNAME** should be the username for your Google account that is able to query M-Lab datasets. If you've never queried our data, please see the M-Lab Quickstart Guide to get setup to do so.

**PUB_LOC** set this to the GCS bucket where you would like your final tilesets and CSVs to be published.

**query_jobs** this is a list of the queries you wish the script to process. Define the queries themselves in `queries/`, and here enter the name of each query, without the `.sql` extension. Query job names should be enclosed in quotes and separated by spaces. We use a backslash at the end of each line for better readability, and to easily comment out queries we might wish to skip by adding a hash sign `#` at the beginning.

In the example above, two queries will be run: `queries/us_state.sql` and `queries/us_aiannh.sql`.

## Required Accounts, Permissions/Roles, etc.

* Google Cloud Project with billing enabled
* Google account with permissions to:
  * Query M-Lab data
  * Create & Delete BigQuery tables in your Google Cloud Project
  * Create & Delete GCS buckets

* Workstation, VM, or other resource to run the script and build the tiles. This system should be well resourced, particularly if you plan to produce maptiles with large sets of features.
  * The builder machine should run a Linux OS such as Ubuntu and needs to have the following packages installed:
    * `sudo apt install python gdal-bin jq libsqlite3-0 zlib1g bash curl git build-essential libsqlite3-dev zlib1g-dev`
    * Compile, build and install _Rust_, _Tippecanoe_, and _xsv_
      * Rust: 
        * `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o tmp/rustup.sh && sh -- /tmp/rustup.sh -y --profile=minimal`
      * Tippecanoe:
        * `git clone https://github.com/mapbox/tippecanoe.git /tmp/tippecanoe-src`
        * `cd /tmp/tippecanoe-src && make && make install`
      * xsv: `~/.cargo/bin/cargo install xsv`
    * Google Cloud SDK installed and authenticated
      * `curl https://sdk.cloud.google.com | bash`
      * `gcloud config set project <your GCP project>`
      * `bq --headless --project_id <your GCP project> ls fake-dataset &> /dev/null || :`


## Building and Using a Docker Container

```sh
docker build --rm -f Dockerfile -t bigquerymaptiles:latest .
```

Run Container (Interactive)

```sh
docker run --rm -it bigquerymaptiles:latest
```

Generate input data using xsv:

```sh
docker run -e PROJECT=mlab-sandbox \
  -v ~/.config/gcloud:/root/.config/gcloud \
  -it bigquerymaptiles:latest ./prep-geojson-input-bq2tiles_ogr_xsv.sh mlab-sandbox
```

NOTE: if the html and tiles are served from different domains we'll need to
apply a CORS policy to GCS.

## CORS

NOTE: may not be needed if served from an iframe.

* create a GCS bucket for the tile data.
* set defacl on bucket:

  ```sh
  gsutil defacl set public-read gs://bigquery-maptiles-mlab-sandbox/
  ```

* set cors policy on bucket, so requests evaluate `Access-Control-Allow-Origin`
  headers correctly.

  ```sh
  gsutil cors set cors.json  gs://bigquery-maptiles-mlab-sandbox
  ```

  `cors.json` contains, a project-specific origin (origin URLs are examples):
  ```
  [
    {
      "origin": ["http://localhost:4000", "https://grafana.mlab-sandbox.measurementlab.net"],
      "responseHeader": ["Content-Type"],
      "method": ["GET", "HEAD", "DELETE"],
      "maxAgeSeconds": 3600
    }
  ]
  ```

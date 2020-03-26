# Build stage: runtime_packages. Share the results of apt-get update across the dev/production images.
FROM ubuntu:19.04 AS runtime_packages
RUN apt-get update && apt-get install -y python gdal-bin jq libsqlite3-0 zlib1g bash

# Build stage: build_environment. Sources and build tools not needed for the ultimate production image.
FROM runtime_packages AS build_environment
RUN apt-get install -y curl git build-essential libsqlite3-dev zlib1g-dev
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh && \
    sh -- /tmp/rustup.sh -y --profile=minimal

## Begin tippecanoe setup steps.
RUN git clone https://github.com/mapbox/tippecanoe.git /tmp/tippecanoe-src
RUN cd /tmp/tippecanoe-src && make && make install
## Tippecanoe fully built at this point.

## Begin xsv setup steps.
RUN ~/.cargo/bin/cargo install xsv
## xsv fully built at this point.

## Install gcloud SDK and command line tools.
RUN curl https://sdk.cloud.google.com | bash
# gcloud installed, leaving some configuration steps for production image.

## Final build stage, containing tools and scripts needed in production job.
FROM runtime_packages

# Copy compiled binaries from build image.
COPY --from=build_environment \
    /root/.cargo/bin/xsv /usr/local/bin/* \
    /root/bin/
COPY --from=build_environment /root/google-cloud-sdk /root/
ENV PATH="${PATH}:/root/google-cloud-sdk/bin:/root/bin"

RUN gcloud config set project measurement-lab
# NOTE: the bq cli leverages the gcloud auth, however still must perform an
# authentication initialization on the first run. This initialization also
# generates an unconditional "Welcome to BigQuery!" preamble message, which
# corrupts the remaining json output. The following command attempts to list a
# fake dataset which runs through the auth initialization and welcome message.
RUN bq --headless --project_id measurement-lab ls fake-dataset &> /dev/null || :

# Copy scripts to generate maptiles.
COPY maptiles queries/ schemas/ scripts/ templates/ /maptiles/
WORKDIR /maptiles

CMD ["./prep-geojson-input-bq2tiles_ogr_xsv.sh"]

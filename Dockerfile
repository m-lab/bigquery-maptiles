FROM ubuntu:19.04
RUN apt-get update && \
    apt-get install -y gdal-bin vim git build-essential libsqlite3-dev \
	    zlib1g-dev golang-go curl python

# Install jsonnet cli tool.
ENV GOPATH /usr/local
RUN go get github.com/google/go-jsonnet/cmd/jsonnet

# Create a directory for tippecanoe sources.
RUN mkdir -p /tmp/tippecanoe-src
RUN git clone https://github.com/mapbox/tippecanoe.git /tmp/tippecanoe-src

# Build tippecanoe.
WORKDIR /tmp/tippecanoe-src
RUN make && make install
WORKDIR /

# Remove source dirs and all build tools.
RUN rm -rf /tmp/tippecanoe-src \
  && apt-get -y remove --purge build-essential && apt-get -y autoremove

# Copy scripts to generate maptiles.
RUN mkdir -p /maptiles
COPY prep-geojson-input.sh /maptiles
COPY query.sql /maptiles
COPY convert.jsonnet /maptiles
COPY example.html /maptiles
WORKDIR /maptiles

# Install gcloud SDK and command line tools.
RUN curl https://sdk.cloud.google.com | bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"

RUN gcloud config set project measurement-lab

# NOTE: the bq cli leverages the gcloud auth, however still must perform an
# authentication initialization on the first run. This initialization also
# generates an unconditional "Welcome to BigQuery!" preamble message, which
# corrupts the remaining json output. The following command attempts to list a
# fake dataset which runs through the auth initialization and welcome message.
RUN bq --headless --project measurement-lab ls fake-dataset &> /dev/null || :

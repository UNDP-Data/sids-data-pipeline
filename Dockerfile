FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install build tools
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git libsqlite3-dev zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

# Install geospatial packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  cython3 python3-dev python3-gdal python3-pip python3-rtree python3-shapely \
  && rm -rf /var/lib/apt/lists/*

# Install tippecanoe
RUN git clone https://github.com/mapbox/tippecanoe.git \
  && cd tippecanoe \
  && make -j \
  && make install \
  && cd ../ \
  && rm -rf tippecanoe

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY processing ./processing

CMD ["python3", "-m", "processing"]

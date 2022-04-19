FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install build tools
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git libsqlite3-dev wget zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

# Install geospatial packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  gdal-bin postgresql-14-postgis-3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

# Install tippecanoe
RUN git clone https://github.com/mapbox/tippecanoe.git \
  && cd tippecanoe \
  && make -j \
  && make install \
  && cd ../ \
  && rm -rf tippecanoe

#  Install AzCopy
RUN wget https://aka.ms/downloadazcopy-v10-linux \
  && tar -xvf downloadazcopy-v10-linux \
  && cp azcopy_linux_amd64_*/azcopy /usr/bin/ \
  && chmod 755 /usr/bin/azcopy \
  && rm -f downloadazcopy-v10-linux \
  && rm -rf azcopy_linux_amd64_*

RUN /etc/init.d/postgresql start \
  && su postgres -c 'createdb sids_data_pipeline' \
  && su postgres -c 'psql -d sids_data_pipeline -c "CREATE EXTENSION postgis;"' \
  && su postgres -c 'psql -d sids_data_pipeline -c "CREATE EXTENSION postgis_raster;"' \
  && su postgres -c 'psql -d sids_data_pipeline -c "ALTER DATABASE sids_data_pipeline SET postgis.enable_outdb_rasters = true;"' \
  && su postgres -c 'psql -d sids_data_pipeline -c "ALTER DATABASE sids_data_pipeline SET postgis.gdal_enabled_drivers TO \'ENABLE_ALL\';"'

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY processing ./processing

CMD /etc/init.d/postgresql start && su postgres -c 'python3 -m processing'

FROM ubuntu:22.04
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates git libsqlite3-dev wget zlib1g-dev \
  gdal-bin postgis postgresql-14-postgis-3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/mapbox/tippecanoe.git \
  && cd tippecanoe \
  && make -j \
  && make install \
  && cd ../ \
  && rm -rf tippecanoe

RUN wget https://aka.ms/downloadazcopy-v10-linux \
  && tar -xvf downloadazcopy-v10-linux \
  && cp azcopy_linux_amd64_*/azcopy /usr/bin/ \
  && chmod 755 /usr/bin/azcopy \
  && rm -f downloadazcopy-v10-linux \
  && rm -rf azcopy_linux_amd64_*

RUN /etc/init.d/postgresql start \
  && su postgres -c 'createdb sids_data_pipeline' \
  && su postgres -c 'psql -d sids_data_pipeline -c "CREATE EXTENSION postgis;"' \
  && su postgres -c 'psql -d sids_data_pipeline -c "CREATE EXTENSION postgis_raster;"'

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY processing ./processing

RUN chown -R postgres:postgres /usr/src/app
CMD /etc/init.d/postgresql start && su postgres -c 'python3 -m processing'

FROM klokantech/tippecanoe as tpc
FROM osgeo/gdal:ubuntu-small-latest as gdal
RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install git build-essential libsqlite3-dev zlib1g-dev python3-pip

RUN python3 -m pip install -U pip
RUN python3 -m pip install pygeoprocessing
RUN python3 -m pip install azure-storage-blob
RUN python3 -m pip install aiohttp




COPY --from=tpc /usr/local/bin/tippecanoe /bin


RUN mkdir /opt/sidspipeline
WORKDIR /opt/sidspipeline
COPY sidspipeline sidspipeline
COPY setup.py .


RUN python3 -m pip install .

CMD sidspipeline



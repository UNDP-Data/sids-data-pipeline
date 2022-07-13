FROM ubuntu:22.04
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  ca-certificates gdal-bin python3 wget \
  && rm -rf /var/lib/apt/lists/*

RUN wget https://aka.ms/downloadazcopy-v10-linux \
  && tar -xvf downloadazcopy-v10-linux \
  && cp azcopy_linux_amd64_*/azcopy /usr/bin/ \
  && chmod 755 /usr/bin/azcopy \
  && rm -f downloadazcopy-v10-linux \
  && rm -rf azcopy_linux_amd64_*

WORKDIR /usr/src/app

COPY processing ./processing

CMD python3 -m processing

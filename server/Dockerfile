FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential ca-certificates curl gnupg python-is-python3 wget \
  && rm -rf /var/lib/apt/lists/*

RUN curl -sL https://nginx.org/keys/nginx_signing.key \
  | gpg --dearmor | tee /usr/share/keyrings/nginx.gpg >/dev/null \
  && echo "deb [signed-by=/usr/share/keyrings/nginx.gpg] https://nginx.org/packages/ubuntu jammy nginx" \
  | tee /etc/apt/sources.list.d/nginx.list
RUN curl -sL https://deb.nodesource.com/gpgkey/nodesource.gpg.key \
  | gpg --dearmor | tee /usr/share/keyrings/nodesource.gpg >/dev/null \
  && echo "deb [signed-by=/usr/share/keyrings/nodesource.gpg] https://deb.nodesource.com/node_16.x jammy main" \
  | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  nginx nodejs \
  && rm -rf /var/lib/apt/lists/*

RUN npm install -g tileserver-gl-light

COPY default.conf /etc/nginx/conf.d/default.conf

WORKDIR /usr/src/app

CMD /etc/init.d/nginx start && tileserver-gl-light --config /usr/src/app/config.json

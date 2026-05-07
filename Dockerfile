FROM postgres:18

RUN apt-get update && apt-get install -y postgresql-server-dev-18 build-essential && \
    cd /extension && make && make install

COPY "extension/" "/extension"
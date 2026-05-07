FROM postgres:18

RUN apt-get update && apt-get install -y postgresql-server-dev-18 build-essential

COPY "extension/" "/extension"

RUN cd /extension && make && make install    

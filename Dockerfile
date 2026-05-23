FROM postgres:18

RUN apt-get update && apt-get install -y \
        postgresql-server-dev-18 \
        build-essential \
        git

# Build pg_ivm (Incrementally Maintainable Materialized Views) from source.
# pg_ivm installs triggers on source tables so IMMVs stay current on every
# INSERT/UPDATE/DELETE — no periodic REFRESH required.
RUN git clone --depth 1 https://github.com/sraoss/pg_ivm.git /tmp/pg_ivm \
    && cd /tmp/pg_ivm \
    && make \
    && make install \
    && rm -rf /tmp/pg_ivm

COPY extension/ /extension/
RUN cd /extension && make clean && make && make install

# Run once on first container start: create pg_stat_statements + pg_ivm.
COPY initdb/ /docker-entrypoint-initdb.d/

# pgbench scripts are available inside both the db and bench containers.
COPY pgbench/ /pgbench/

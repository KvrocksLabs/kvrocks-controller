FROM postgres:16

RUN apt-get update && apt-get install -y \
    postgresql-server-dev-16 \
    build-essential \
    libpq-dev \
    wget

RUN wget https://github.com/citusdata/pg_cron/archive/refs/tags/v1.6.5.tar.gz \
    && tar -xvzf v1.6.5.tar.gz \
    && cd pg_cron-1.6.5 \
    && make && make install \
    && cd .. && rm -rf v1.6.5.tar.gz pg_cron-1.6.5

RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample \
    && echo "cron.database_name = 'testdb'" >> /usr/share/postgresql/postgresql.conf.sample
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
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
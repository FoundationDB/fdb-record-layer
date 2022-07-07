--
-- testConnectAndQuery.sql
--
-- This source file is part of the FoundationDB open source project
--
-- Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

connect 'jdbc:embed:/__SYS';
-- hmm..are we connected? Lets see
show connections;
disconnect;

-- doing that all the time is boring, lets put it in a different script
execute '/Users/scottfines/workspace/relational/fdb-relational-cli/src/test/resources/sql/testConnectAndShow.sql';

-- ok, let's look at some simple table data
set schema 'catalog';
select * from 'SCHEMAS';
-- that table looks a little tight, let's widen it
config 'consoleWidth'='180';
select * from 'SCHEMAS';

-- ok, but what if we want it in JSON?

config 'format'='json';
select * from 'SCHEMAS';

-- pretty json is bad for machines, lets make it more compact
config 'prettyPrint'='false';
select * from 'SCHEMAS';

-- I wonder if we can limit the rows to oblivion?
config 'displayLimit'='0';
select * from 'SCHEMAS';

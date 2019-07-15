/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Classes relating to hooking up join index synthetic records to the query planner.
 *
 * <p>
 * These planners and plans are used to generate synthetic records for the purpose of maintaining
 * indexes of them. They run in response to changes that require such maintenance. For example, when
 * a new index on a synthetic record type is defined, all its records need to be synthesized to populate
 * the index. When an ordinary stored record that is part of a synthetic record type is modified, the
 * associated records must be synthesized to update the index.
 * </p>
 *
 * <p>
 * These classes are <em>not</em> used as part of ordinary query planning and execution. Rather, they use
 * the ordinary query planner, building queries from the synthetic record meta-data and running those queries
 * to produce streams of stored records that are combined into streams of synthetic records for indexing.
 * Since the query planner does not support joins, the synthetic planners must build multiple query plans
 * and nest them. It is expected that this will change substantially when the planner itself supports joins.
 * </p>
 */
package com.apple.foundationdb.record.query.plan.synthetic;

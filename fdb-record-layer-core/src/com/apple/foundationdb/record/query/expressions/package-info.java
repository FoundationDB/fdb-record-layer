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
 * Classes implementing logical query expression.
 *
 * <p>
 * A {@link com.apple.foundationdb.record.query.expressions.QueryComponent} represents a boolean predicate against a record in the record store, like the {@code WHERE} clause in SQL databases.
 * Query expressions are built using static methods on {@link com.apple.foundationdb.record.query.expressions.Query} and then passed to {@link com.apple.foundationdb.record.query.RecordQuery.Builder#setFilter setFilter}.
 * </p>
 *
 * <p>
 * For example,
 * </p>
 * <pre><code>
 * queryBuilder.setFilter(Query.and(
 *     Query.field("field1").equalsParameter("p1"),
 *     Query.field("field2").greaterThan(100)))
 * </code></pre>
 */
package com.apple.foundationdb.record.query.expressions;

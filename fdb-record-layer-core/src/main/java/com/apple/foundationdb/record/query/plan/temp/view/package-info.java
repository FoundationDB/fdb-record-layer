/*
 * package-info.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
 * Classes related to the {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpression} abstraction.
 *
 * <p>
 * A view expression is an abstraction that represents a stream of values that can be represented as tuple-encoded keys
 * and binary values in FoundationDB. View expressions are quite general and can represent a large variety of index
 * structures and even queries. However, because they are <em>defined</em> by their close relationship to the FDB
 * tuple layer encoding, they are extremely useful for many query planning operations, such as incremental index
 * selection (for example, see {@link com.apple.foundationdb.record.query.plan.temp.rules.PushElementWithComparisonIntoExistingScanRule}).
 * </p>
 *
 * <p>
 * Semantically, a view expression can be read much like a query in a declarative query language like SQL. For example,
 * it can specify a projection, sort order, and filter predicate. When used to represent the structure of an index,
 * a view expression can be interpreted as a query for which the index acts as a materialized view.
 * {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpression} describes the specific semantics in further
 * detail.
 * </p>
 *
 * <p>
 * The two key abstractions that are used for defining view expressions are {@link com.apple.foundationdb.record.query.plan.temp.view.Source}s
 * and {@link com.apple.foundationdb.record.query.plan.temp.view.Element}s. A source represents a stream of entities
 * that can be manipulated to form a view expression, while an element defines the mapping from one entry in a source's
 * stream to a value that can be represented as part of a tuple. For example, the
 * {@link com.apple.foundationdb.record.query.plan.temp.view.RecordTypeSource} represents a stream of records with
 * types from a specified set of record types. A {@link com.apple.foundationdb.record.query.plan.temp.view.FieldElement}
 * produces values from a field of each entry in a stream of records.
 * </p>
 *
 * <p>
 * During query planning with the {@link com.apple.foundationdb.record.query.plan.temp Cascades-style planner},
 * {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison}s are matched to elements of a view
 * expression. The state and logic for determining which comparisons can be matched to a (partially matched) view
 * expression is encapsulated in the {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons}
 * class.
 * </p>
 */
package com.apple.foundationdb.record.query.plan.temp.view;

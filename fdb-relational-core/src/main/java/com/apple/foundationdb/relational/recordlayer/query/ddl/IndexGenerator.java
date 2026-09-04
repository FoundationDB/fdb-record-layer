/*
 * IndexGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;

import javax.annotation.Nonnull;

/**
 * Turns an index definition into the metadata that stores it.
 * <p>
 * A {@code CREATE INDEX} statement arrives as a parse tree, which {@link com.apple.foundationdb.relational.recordlayer.query.visitors.DdlVisitor}
 * visits. Whatever the syntax, the definition is first expressed as a logical plan -- a tree of
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression}s rooted at a
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression}, unoptimised, since it
 * describes what to store rather than how to read it. An implementation of this interface reads that plan and produces a
 * {@link RecordLayerIndex}: a name, the type of index, the table it is on, the predicate it is filtered by, and the
 * {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} its entries are keyed and valued by. That
 * index becomes part of the schema template the statement is adding to.
 * </p>
 * <p>
 * There is one implementation per definition syntax, and the plan is where they meet:
 * </p>
 * <ul>
 *     <li>{@code CREATE INDEX mv1 AS SELECT min_ever(col3) FROM T2 GROUP BY col1, col2} is planned as written, and
 *     {@link MaterializedViewIndexGenerator} reads the plan directly. It yields a {@code MIN_EVER_TUPLE} index keyed by
 *     {@code field("COL3").groupBy(field("COL1"), field("COL2"))}.</li>
 *     <li>{@code CREATE VECTOR INDEX mv1 USING HNSW ON v1(b) PARTITION BY (z)} has no query to plan, so
 *     {@link OnSourceIndexGenerator} synthesises one -- a select over the table projecting the named columns, sorted by
 *     the partitioning ones -- and hands it to {@link MaterializedViewIndexGenerator}. It yields a {@code VECTOR} index
 *     keyed by {@code keyWithValue(concat(field("Z"), field("B")), 1)}, the vector sitting in the value.</li>
 * </ul>
 * <p>
 * What this interface provides is that last step alone: given a definition, the metadata for it. An implementation
 * carries the whole definition -- the plan, the name, the table, and what the definition asks for beyond its key -- so
 * generating takes no further input, and the syntax it came from stops being visible to the caller.
 * </p>
 */
public interface IndexGenerator {

    /**
     * Builds the index this generator was configured with.
     * <p>
     * The returned builder is not yet built, so a caller that knows something the definition does not carry can still
     * add it -- a vector index, for instance, sets its index type and engine options afterwards. Nothing is written to
     * the schema template until the caller builds it and adds it.
     * </p>
     *
     * @return the index the definition asks for
     *
     * @throws com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException with
     * {@link com.apple.foundationdb.relational.api.exceptions.ErrorCode#UNSUPPORTED_OPERATION} if the definition
     * describes an index that cannot be stored, such as one over a join, or one whose projection holds a value that no
     * key expression can express
     */
    @Nonnull
    RecordLayerIndex.Builder generate();
}

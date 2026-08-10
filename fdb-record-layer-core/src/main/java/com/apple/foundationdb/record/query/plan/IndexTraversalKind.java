/*
 * IndexTraversalKind.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;

import javax.annotation.Nonnull;

/**
 * The way a plan physically traverses an index, as opposed to the logical
 * {@link IndexTypes index type} the index is declared as. The distinction is
 * not just a renaming of the index type: what a scan does at runtime is a property of how it is scanned, not only of
 * what
 * it is scanned on. A {@link IndexTypes#RANK rank} index scanned
 * {@link IndexScanType#BY_RANK} walks a probabilistic skip list, while the very same index scanned
 * {@link IndexScanType#BY_VALUE} walks ordered key-value pairs, and those two have little in common in terms of what
 * they cost.
 * <p>
 * This is what lets planning and costing reason about an index access by what it will actually do. Two accesses that
 * are indistinguishable to the cost model may traverse structures with very different access characteristics, and a
 * cost criterion, or an explicitly stated preference, may want to tell them apart.
 * <p>
 * This isn't an enum, for the same reason {@link IndexScanType} isn't: clients can define index maintainers of their
 * own,
 * and a maintainer that scans something the core has never heard of needs to be able to name what it walks. Declare a
 * constant for it as the core does below, and answer with it from
 * {@link IndexScanParameters#getIndexTraversalKind()}. Kinds are equal exactly when their names are, so a consumer
 * should compare with {@link #equals(Object)} and always have an answer for a kind it does not recognize.
 *
 * @param name The name of the index traversal kind.
 *
 * @see IndexScanParameters#getIndexTraversalKind()
 * @see RecordQueryIndexPlan#getIndexTraversalKind()
 */
@API(API.Status.EXPERIMENTAL)
public record IndexTraversalKind(@Nonnull String name) {
    /**
     * A traversal that is not known to whoever was asked, either because the scan is described by an
     * {@link IndexScanType} that layer does not define, or because it has not been taught to say. A consumer keyed on a
     * specific traversal should treat this as "not the one I am looking for" rather than as any particular one.
     */
    @Nonnull
    public static final IndexTraversalKind UNKNOWN = new IndexTraversalKind("UNKNOWN");
    /**
     * Ordered key-value pairs walked in key order, which is to say FoundationDB's keyspace scanned directly. This is
     * what {@link IndexScanType#BY_VALUE} and {@link IndexScanType#BY_VALUE_OVER_SCAN} do, on any index whose entries
     * are plain key-value pairs, and also what {@link IndexScanType#BY_GROUP} does when it reads the aggregate held
     * against a group key.
     */
    @Nonnull
    public static final IndexTraversalKind BY_VALUE = new IndexTraversalKind("BY_VALUE");
    /**
     * A probabilistic skip list walked to resolve an ordinal rank, rather than a key range, into entries. This is what
     * {@link IndexScanType#BY_RANK} and {@link IndexScanType#BY_TIME_WINDOW} do.
     */
    @Nonnull
    public static final IndexTraversalKind RANKED_SET = new IndexTraversalKind("RANKED_SET");
    /**
     * A postings structure walked from a token to the entries containing it, as {@link IndexScanType#BY_TEXT_TOKEN}
     * does.
     */
    @Nonnull
    public static final IndexTraversalKind INVERTED = new IndexTraversalKind("INVERTED");
    /**
     * An R-tree walked for overlap or containment on multi-dimensional data, as a
     * {@link MultidimensionalIndexScanComparisons multidimensional}
     * scan does.
     */
    @Nonnull
    public static final IndexTraversalKind R_TREE = new IndexTraversalKind("R_TREE");
    /**
     * A hierarchical navigable small world graph walked for approximate nearest neighbors.
     */
    @Nonnull
    public static final IndexTraversalKind HNSW = new IndexTraversalKind("HNSW");
    /**
     * A clustered vector structure walked for approximate nearest neighbors.
     */
    @Nonnull
    public static final IndexTraversalKind GUARDIANN = new IndexTraversalKind("GUARDIANN");

    /**
     * Returns the name of this traversal, which is what distinguishes it from every other one.
     *
     * @return the name of this traversal
     */
    @Override
    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    @Override
    public String toString() {
        return name;
    }
}

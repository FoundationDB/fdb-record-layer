/*
 * PhysicalIndexKind.java
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
import com.apple.foundationdb.record.planprotos.PPhysicalIndexKind;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.collect.BiMap;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;

/**
 * The kind of physical structure backing an index, as opposed to the logical
 * {@link com.apple.foundationdb.record.metadata.IndexTypes index type} it is declared as. Several index types share a
 * structure — every aggregate index is ultimately ordered key-value pairs, just like a value index — while a single index
 * type can be backed by more than one structure, as a {@code vector} index is by either HNSW or Guardiann.
 * <p>
 * This is what lets planning and costing reason about an access by what it will actually do at runtime. Two plans that
 * are indistinguishable to the cost model may sit on structures with very different access characteristics, and a cost
 * criterion, or an explicitly stated preference, may want to tell them apart.
 * <p>
 * The values that are not a structure at all, {@link #NONE} and {@link #MIXED}, exist because a plan is asked for its
 * kind as a whole, not just an individual access: a plan may reach no index, or several of differing kinds. See
 * {@link #combine(Iterable)}.
 */
@API(API.Status.EXPERIMENTAL)
public enum PhysicalIndexKind {
    //
    // UNKNOWN is deliberately first so that it stays at ordinal zero however many kinds are added later: it is the
    // value a consumer should fall back to, and the one a serialized form defaults to when it carries nothing.
    //
    /**
     * A structure that is not known here, because whoever supplies the kind did not declare one. A consumer keyed on a
     * specific structure should treat this as "not the one I am looking for" rather than as any particular kind.
     */
    UNKNOWN,
    /**
     * The primary keyspace, reached directly rather than through an index — a full record scan, or an operator that does
     * not read records at all. Physically this is ordered key-value pairs just as {@link #BTREE} is; it is called out
     * separately because no index is involved, which is usually the distinction a consumer cares about.
     */
    PRIMARY,
    /**
     * Ordered key-value pairs, which is to say FoundationDB's keyspace used directly. Backs
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE value} and
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#VERSION version} indexes, every atomic-mutation
     * aggregate ({@code count}, {@code sum}, {@code min_ever}, {@code max_ever} and friends), the permuted
     * {@code min}/{@code max} indexes, and {@code bitmap_value}, whose bitmaps are values in exactly such a keyspace.
     */
    BTREE,
    /**
     * A probabilistic skip list over the keyspace, giving ordinal rank and select in addition to ordered access. Backs
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#RANK rank} and
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#TIME_WINDOW_LEADERBOARD time_window_leaderboard}.
     */
    RANKED_SET,
    /**
     * A postings structure mapping tokens to the records containing them. Backs
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#TEXT text}.
     */
    INVERTED,
    /**
     * An R-tree over the keyspace, for overlap and containment on multi-dimensional data. Backs
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#MULTIDIMENSIONAL multidimensional}.
     */
    R_TREE,
    /**
     * A space-filling curve linearizing multi-dimensional data into the ordered keyspace. Backs the
     * {@code spatial_geophile} index type of the spatial module.
     */
    SPACE_FILLING_CURVE,
    /**
     * A hierarchical navigable small world graph, for approximate nearest-neighbor search. Backs a
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index whose engine is HNSW.
     */
    HNSW,
    /**
     * A clustered vector structure, for approximate nearest-neighbor search. Backs a
     * {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index whose engine is Guardiann.
     */
    GUARDIANN,
    /**
     * A Lucene index, held in a directory of its own rather than as plain key-value pairs. Backs the {@code lucene}
     * index type of the Lucene module.
     */
    LUCENE,
    /**
     * An in-memory structure rather than anything persistent, as a temporary table is. Combining this with another kind
     * yields that other kind rather than {@link #MIXED}: it is not a persistent structure a plan relies on, so it says
     * nothing about which one the plan is built around. See {@link #combine(Iterable)}.
     */
    IN_MEMORY,
    /**
     * More than one kind, i.e. a plan whose accesses do not agree on a structure. Deliberately not a set of the kinds
     * involved: a consumer that needs that detail should look at the individual accesses rather than at the plan.
     */
    MIXED;

    @Nonnull
    private static final BiMap<PhysicalIndexKind, PPhysicalIndexKind> TO_PROTO =
            PlanSerialization.protoEnumBiMap(PhysicalIndexKind.class, PPhysicalIndexKind.class);

    /**
     * Combines the kinds of several accesses, or of the children of a plan, into the kind of the whole. An empty
     * argument yields {@link #UNKNOWN}, kinds that all agree yield that kind, and anything else yields {@link #MIXED}.
     * <p>
     * {@link #IN_MEMORY} is the one identity: it drops out of the combination, so {@code BTREE} combined with it is
     * still {@code BTREE}, and only a combination of nothing but {@code IN_MEMORY} stays {@code IN_MEMORY}. A temporary
     * table alongside an index scan does not make a plan ambiguous about which persistent structure it is built around.
     * <p>
     * {@link #PRIMARY}, by contrast, is <em>not</em> an identity: combining it with another kind yields {@link #MIXED}.
     * A union of a full scan and an index scan genuinely is a plan of two minds about how it reaches records, and
     * reporting it as though it were purely the one kind would overstate what is known about it.
     *
     * @param kinds the kinds to combine
     * @return the kind of the whole
     */
    @Nonnull
    public static PhysicalIndexKind combine(@Nonnull final Iterable<PhysicalIndexKind> kinds) {
        PhysicalIndexKind combined = null;
        boolean sawInMemory = false;
        for (final PhysicalIndexKind kind : kinds) {
            if (kind == IN_MEMORY) {
                sawInMemory = true;
                continue;
            }
            if (combined == null) {
                combined = kind;
            } else if (combined != kind) {
                return MIXED;
            }
        }
        if (combined != null) {
            return combined;
        }
        return sawInMemory ? IN_MEMORY : UNKNOWN;
    }

    /**
     * Combines the kinds of several accesses, or of the children of a plan, into the kind of the whole.
     *
     * @param kinds the kinds to combine
     * @return the kind of the whole
     * @see #combine(Iterable)
     */
    @Nonnull
    public static PhysicalIndexKind combine(@Nonnull final PhysicalIndexKind... kinds) {
        return combine(Arrays.asList(kinds));
    }

    /**
     * Converts this kind to its protobuf equivalent, so that a plan can carry it.
     *
     * @return the protobuf equivalent of this kind
     */
    @Nonnull
    public PPhysicalIndexKind toProto() {
        return Objects.requireNonNull(TO_PROTO.get(this));
    }

    /**
     * Converts a kind back from its protobuf equivalent.
     *
     * @param physicalIndexKindProto the protobuf kind
     * @return the kind
     */
    @Nonnull
    public static PhysicalIndexKind fromProto(@Nonnull final PPhysicalIndexKind physicalIndexKindProto) {
        return Objects.requireNonNull(TO_PROTO.inverse().get(physicalIndexKindProto));
    }
}

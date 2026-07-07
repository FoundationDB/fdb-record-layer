/*
 * VectorId.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Identifies a stored vector by the {@code primaryKey} of the record it was indexed from, together with a
 * {@code uuid} that marks a particular <em>generation</em> of that vector. The primary key is stable for the life of
 * the record; the UUID is assigned once, when the vector is inserted, and is then carried unchanged through every
 * later write.
 *
 * <p>
 * The UUID exists so stale copies can be <em>detected and filtered</em> rather than hunted down. A single vector lives
 * in several physical places at once — a primary copy in its home cluster plus replicated copies scattered across
 * neighboring clusters — and every copy belonging to one generation carries that generation's UUID. Maintenance
 * (moving a primary to a nearer cluster, adding or pruning replicas during split/merge/reassign) always carries the
 * existing UUID forward, so the UUID does <em>not</em> distinguish a primary from its replicas, nor an original copy
 * from one a later task produced — they all share it. A new UUID is minted only by an {@code insert} of the primary
 * key: updating a record re-inserts its vector under a new UUID and overwrites the authoritative per-record
 * {@link VectorMetadata} (keyed by primary key) to point at that new generation.
 *
 * <p>
 * The previous generation's copies ought to be deleted, but nothing keeps an exhaustive index of where a vector's
 * replicas landed, and delete only probes a bounded number of nearest clusters (see
 * {@link Config#deleteMaxCandidateClusters()}) — both replication and delete are approximate — so earlier-generation
 * copies might generally linger. Correctness does not hinge on removing them: because {@link VectorMetadata} records
 * the <em>current</em> generation's UUID, any reference whose UUID does not match it is recognized as stale and
 * dropped, both at search time and during the reassign/split/merge clean-up path. The primary key alone cannot serve
 * this role, since every generation of the record shares it.
 *
 * <p>
 * {@code VectorId} implements {@link Comparable} only to give the distance- and replication-priority comparators used
 * across this package a total, deterministic tie-breaker (they sort by their real key, then fall back to the id).
 * Ordering by UUID is otherwise arbitrary and carries no semantic meaning; it exists so those sorts come out total and
 * reproducible.
 *
 * @param primaryKey the primary key of the record this vector was indexed from; stable across the record's lifetime
 * @param uuid a marker for one generation of the vector — assigned at insert, carried unchanged through every later
 *        maintenance write, and replaced only when the record is re-inserted (updated); shared by every copy of that
 *        generation, so stale copies from an earlier generation can be filtered by UUID mismatch
 */
record VectorId(@Nonnull Tuple primaryKey, @Nonnull UUID uuid) implements Comparable<VectorId> {
    @Nonnull
    @Override
    public String toString() {
        return "VId[" + primaryKey + ";" + uuid + "]";
    }

    @Override
    public int compareTo(@Nonnull final VectorId o) {
        final int cmp = primaryKey.compareTo(o.primaryKey());
        if (cmp != 0) {
            return cmp;
        }
        return uuid.compareTo(o.uuid());
    }
}

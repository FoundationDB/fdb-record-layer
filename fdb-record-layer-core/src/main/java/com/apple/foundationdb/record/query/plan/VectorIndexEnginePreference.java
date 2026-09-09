/*
 * VectorIndexEnginePreference.java
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

/**
 * Which vector index engine the planner should favor when a query could be answered by more than one
 * {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index and the alternatives are otherwise
 * indistinguishable — most notably when the same field carries both an
 * {@link com.apple.foundationdb.async.hnsw.HNSW HNSW}-backed and a
 * {@link com.apple.foundationdb.async.guardiann.Guardiann Guardiann}-backed index with the same metric.
 * <p>
 * This is a stated preference, not a cost estimate. There is nothing that would let the planner judge one engine's
 * search to be cheaper or more accurate than another's, so without a preference the choice falls to the cost model's
 * tie-break, which is stable across replannings but keys on the index name and is therefore arbitrary. Expressing the
 * choice here instead keeps it visible and reversible: it takes no metadata change and no index rebuild to move queries
 * from one engine to the other and back.
 * <p>
 * Possible values are:
 * <UL>
 * <LI>{@link #NO_PREFERENCE} leave the choice to the cost model, i.e. the behavior from before this option existed.
 * This is the default, so that merely creating an index of the other engine cannot silently move existing queries onto
 * it.</LI>
 * <LI>{@link #PREFER_HNSW} favor a vector index plan over an HNSW-backed index.</LI>
 * <LI>{@link #PREFER_GUARDIANN} favor a vector index plan over a Guardiann-backed index.</LI>
 * </UL>
 * <p>
 * A preference is never a requirement: a query whose only candidate is an index of the non-preferred engine still
 * plans, and a query with no vector index candidate at all is unaffected.
 */
@API(API.Status.EXPERIMENTAL)
public enum VectorIndexEnginePreference {
    NO_PREFERENCE,
    PREFER_HNSW,
    PREFER_GUARDIANN
}

/*
 * LeafQueryPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * Class to model the concept of a predicate. A predicate is a construct that can be evaluated using
 * three-values logic for a set of given inputs. The caller can then use that result to take appropriate action,
 * e.g. filter a record out of a set of records, etc.
 */
@API(API.Status.EXPERIMENTAL)
public interface LeafQueryPredicate extends QueryPredicate {
    @Nonnull
    @Override
    default Iterable<? extends QueryPredicate> getChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    default QueryPredicate withChildren(final Iterable<? extends QueryPredicate> newChildren) {
        return this;
    }

    /**
     * Default implementation to be mixed in.
     * @return the result of invoking {@link #hashCodeWithoutChildren()}
     */
    default int computeSemanticHashCode() {
        return hashCodeWithoutChildren();
    }

    @Nonnull
    @Override
    default QueryPredicate withAtomicity(boolean isAtomic) {
        return this;
    }

}

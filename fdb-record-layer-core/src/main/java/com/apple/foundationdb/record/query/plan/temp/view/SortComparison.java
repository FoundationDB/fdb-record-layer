/*
 * SortComparison.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class SortComparison implements Comparisons.Comparison {
    /**
     * A "comparison" that represents a sort on the given field, based on the observation that the planner can generally
     * treat a requested sort order as a pair of comparisons requiring the field to be greater than "negative infinity"
     * and less than "positive infinity".
     *
     * @see ViewExpressionComparisons
     */
    @Nonnull
    static final Comparisons.Comparison SORT = new SortComparison();

    @Nullable
    @Override
    public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
        return Boolean.TRUE;
    }

    @Override
    public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
    }

    @Nonnull
    @Override
    public Comparisons.Type getType() {
        return Comparisons.Type.SORT;
    }

    @Nullable
    @Override
    public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
        return null;
    }

    @Nullable
    @Override
    public Object getComparand() {
        return null;
    }

    @Nonnull
    @Override
    public String typelessString() {
        return "SORT";
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return 0;
    }
}

/*
 * AndPredicate.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A {@link QueryPredicate} that is satisfied when all of its child components are;
 *
 * For tri-valued logic:
 * <ul>
 * <li>If all children are {@code true}, then {@code true}.</li>
 * <li>If any child is {@code false}, then {@code false}.</li>
 * <li>Else {@code null}.</li>
 * </ul>
 */
public class AndPredicate extends AndOrPredicate {
    public AndPredicate(@Nonnull List<QueryPredicate> children) {
        super(children);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                            @Nonnull SourceEntry sourceEntry) {
        Boolean defaultValue = Boolean.TRUE;
        for (QueryPredicate child : getChildren()) {
            final Boolean val = child.eval(store, context, sourceEntry);
            if (val == null) {
                defaultValue = null;
            } else if (!val) {
                return false;
            }
        }
        return defaultValue;
    }

    @Override
    public String toString() {
        return "And(" + getChildren() + ")";
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        // TODO: Shouldn't there be anything for the "AND" part too?
        return PlanHashable.planHash(hashKind, getChildren());
    }

    @Override
    public AndPredicate rebaseWithRebasedChildren(final AliasMap translationMap, final List<QueryPredicate> rebasedChildren) {
        return new AndPredicate(rebasedChildren);
    }
}

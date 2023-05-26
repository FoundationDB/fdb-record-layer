/*
 * PickValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A value representing multiple "alternative" values.
 * This is useful to element SQL-values-like behavior, e.g.
 * <pre>
 * {@code
 * VALUES (1, "Hello World", 3.0),
 *        (2, "Lazy Dag", 6.5),
 *        (10, "Brown Fow", -2.3)
 * }
 * can be rewritten as
 * {@code
 * SELECT PICK(range.index, (1, "Hello World", 3.0), RCV(2, "Lazy Dag", 6.5), (10, "Brown Fow", -2.3))
 * FROM RANGE(3) range
 * }
 * </pre>
 * of their {@link Value}s
 */
@API(API.Status.EXPERIMENTAL)
public class PickValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Pick-Value");
    @Nonnull
    private final Value selectorValue;
    @Nonnull
    private final List<? extends Value> alternativeValues;
    @Nonnull
    private final Iterable<? extends Value> children;
    @Nonnull
    private final Type resultType;

    public PickValue(@Nonnull final Value selectorValue, @Nonnull final Iterable<? extends Value> alternativeValues) {
        this.selectorValue = selectorValue;
        this.alternativeValues = ImmutableList.copyOf(alternativeValues);
        this.children = Iterables.concat(ImmutableList.of(selectorValue), alternativeValues);
        this.resultType = resolveTypesFromAlternatives(alternativeValues);
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var boxedSelectedIndex = (Integer)selectorValue.eval(store, context);
        if (boxedSelectedIndex == null) {
            return null;
        }

        final var selectedIndex = (int)boxedSelectedIndex;
        return alternativeValues.get(selectedIndex).eval(store, context);
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenIterator = newChildren.iterator();
        final var newSelectorValue = newChildrenIterator.next(); // must exist
        // this skips the very first child for the alternatives as that one is the selector
        return new PickValue(newSelectorValue, ImmutableList.copyOf(newChildrenIterator));
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        return alternativeValues.stream()
                .allMatch(alternativeValue -> alternativeValue.isFunctionallyDependentOn(otherValue));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getChildren());
    }

    @Override
    public String toString() {
        return "PickValue";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    private static Type resolveTypesFromAlternatives(@Nonnull final Iterable<? extends Value> alternativeValues) {
        Type commonType = null;
        for (final var alternativeValue : alternativeValues) {
            final var resultType = alternativeValue.getResultType();
            if (commonType == null) {
                commonType = resultType;
            } else {
                SemanticException.check(commonType.equals(resultType), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
        }
        return Verify.verifyNotNull(commonType); // throws if there are no alternatives
    }
}

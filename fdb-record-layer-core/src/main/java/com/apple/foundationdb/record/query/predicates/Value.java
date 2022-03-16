/*
 * Value.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Correlated;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.CreatesDynamicTypesValue;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.temp.Narrowable;
import com.apple.foundationdb.record.query.plan.temp.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.temp.TreeLike;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.Typed;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A scalar value type.
 */
@API(API.Status.EXPERIMENTAL)
public interface Value extends Correlated<Value>, TreeLike<Value>, PlanHashable, KeyExpressionVisitor.Result, Typed, Narrowable<Value> {

    @Nonnull
    @Override
    default Value getThis() {
        return this;
    }

    /**
     * Returns the {@link Type} of the scalar value output.
     * @return The {@link Type} of the scalar value output.
     */
    @Nonnull
    @Override
    default Type getResultType() {
        return Type.primitiveType(Type.TypeCode.UNKNOWN);
    }

    /**
     * Iterates over the entire expression tree collecting a set of all dynamically-generated types.
     * A {@link Type} could be generated dynamically by a {@link Value} that e.g. encapsulates children {@link Type}s
     * into a single structured type such as a {@link com.apple.foundationdb.record.query.plan.temp.Type.Record}.
     *
     * For more information, check implementations of {@link CreatesDynamicTypesValue} interface.
     *
     * @return A set of dynamically-generated {@link Type} by this {@link Value} and all of its children.
     */
    @SuppressWarnings("java:S4738")
    @Nonnull
    default Set<Type> getDynamicTypes() {
        return fold(p -> {
            if (p instanceof CreatesDynamicTypesValue) {
                return ImmutableSet.of(p.getResultType());
            }
            return ImmutableSet.<Type>of();
        }, (thisTypes, childTypeSets) -> {
            final ImmutableSet.Builder<Type> nestedBuilder = ImmutableSet.builder();
            for (final Set<Type> childTypes : childTypeSets) {
                nestedBuilder.addAll(childTypes);
            }
            nestedBuilder.addAll(thisTypes);
            return nestedBuilder.build();
        });
    }

    /**
     * evaluates computation of the expression at compile time and returns the result immediately.
     *
     * @param context The execution context.
     * @return The expression output.
     */
    @Nullable
    @SuppressWarnings({"java:S2637", "ConstantConditions"})
    @SpotBugsSuppressWarnings(value = {"NP_NONNULL_PARAM_VIOLATION"}, justification = "compile-time evaluations take their value from the context only")
    default Object compileTimeEval(@Nonnull final EvaluationContext context) {
        return eval(null, context, null, null);
    }

    @Nullable
    <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable FDBRecord<M> record, @Nullable M message);

    /**
     * Method to create a {@link QueryPredicate} that is based on this value and a
     * {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} that is passed in by the
     * caller.
     * @param comparison comparison to relate this value to
     * @return a new {@link ValuePredicate} using the passed in {@code comparison}
     */
    @Nonnull
    default ValuePredicate withComparison(@Nonnull Comparisons.Comparison comparison) {
        return new ValuePredicate(this, comparison);
    }

    /**
     * Method to create a {@link Placeholder} that is based on this value. A placeholder is also a {@link QueryPredicate}
     * that is used solely for matching query predicates against.
     * @param parameterAlias alias to uniquely identify the parameter in the
     *        {@link com.apple.foundationdb.record.query.plan.temp.MatchCandidate} this placeholder will be a part of.
     * @return a new {@link Placeholder}
     */
    @Nonnull
    default Placeholder asPlaceholder(@Nonnull final CorrelationIdentifier parameterAlias) {
        return ValueComparisonRangePredicate.placeholder(this, parameterAlias);
    }

    /**
     * Method to derive if this value is functionally dependent on another value. In order to produce a meaningful
     * result that {@code otherValue} and this value must parts of the result values of the same
     * {@link com.apple.foundationdb.record.query.plan.temp.RelationalExpression}.
     *
     * <h2>Example 1</h2>
     * <pre>
     * {@code
     *    SELECT q, q.x, q.y
     *    FROM T q
     * }
     * </pre>
     * {@code q.x} and {@code q.y} are both functionally dependent on {@code q} meaning that for a given quantified
     * (bound) value of {@code q} there is exactly one scalar result for this value. In other words, {@code q -> q.x}
     * and {@code q -> q.y}
     *
     * <h2>Example 2</h2>
     * <pre>
     * {@code
     *    SELECT q1, q1.x, q2.y
     *    FROM S q1, T q2
     * }
     * </pre>
     * {@code q1.x} is functionally dependent on {@code q1} meaning that for a given quantified (bound) value of
     * {@code q1} there is exactly one scalar result for this value. In other words, {@code q1 -> q1.x}.
     * {@code q2.x} is functionally dependent on {@code q2} meaning that for a given quantified (bound) value of
     * {@code q2} there is exactly one scalar result for this value. In other words, {@code q2 -> q2.x}.
     * Note that it does not hold that {@code q1 -> q2.y} nor that {@code q2 -> q1.x}.
     *
     * <h2>Example 3</h2>
     * <pre>
     * {@code
     *    SELECT q1, q2.y
     *    FROM S q1, (SELECT * FROM EXPLODE(q1.x) q2
     * }
     * </pre>
     * {@code q2.y} is not functionally dependent on {@code q1} as for a given quantified (bound) value of {@code q1}
     * there may be many or no associated values over {@code q2}.
     *
     * <h2>Example 4</h2>
     * <pre>
     * {@code
     *    SELECT q1, 3
     *    FROM S q1, (SELECT * FROM EXPLODE(q1.x) q2
     * }
     * {@code 3} is functionally dependent on {@code q1} as for any given quantified (bound) value of {@code q1}
     * there is exactly one scalar result for this value (namely {@code 3}).*
     * </pre>
     *
     * Note that if {@code x -> y} and {@code y -> z} then {@code x -> z} should hold. Note that this method attempts
     * a best effort to establish the functional dependency relationship between the other value and this value. That
     * means that the caller cannot rely on a {@code false} result to deduce that this value is definitely not
     * dependent on {@code otherValue}.
     *
     * @param otherValue other value to check if this value is functionally dependent on it
     * @return {@code true} if this value is definitely dependent on {@code otherValue}
     */
    default boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        if (!(otherValue instanceof QuantifiedValue)) {
            return false;
        }

        return StreamSupport.stream(inPreOrder().spliterator(), false)
                .flatMap(value -> value instanceof QuantifiedValue ? Stream.of((QuantifiedValue)value) : Stream.empty())
                .allMatch(quantifiedValue -> quantifiedValue.isFunctionallyDependentOn(otherValue));
    }

    @Nonnull
    @Override
    default Set<CorrelationIdentifier> getCorrelatedTo() {
        return fold(Value::getCorrelatedToWithoutChildren,
                (correlatedToWithoutChildren, childrenCorrelatedTo) -> {
                    ImmutableSet.Builder<CorrelationIdentifier> correlatedToBuilder = ImmutableSet.builder();
                    correlatedToBuilder.addAll(correlatedToWithoutChildren);
                    childrenCorrelatedTo.forEach(correlatedToBuilder::addAll);
                    return correlatedToBuilder.build();
                });
    }

    @Nonnull
    default Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    default Value rebase(@Nonnull final AliasMap translationMap) {
        return replaceLeavesMaybe(t -> t.rebaseLeaf(translationMap)).orElseThrow(() -> new RecordCoreException("unable to map tree"));
    }

    @Nonnull
    @SuppressWarnings("unused")
    default Value rebaseLeaf(@Nonnull final AliasMap translationMap) {
        throw new RecordCoreException("implementor must override");
    }

    @Nonnull
    default Optional<Value> translate(@Nonnull final Map<Value, Value> translationMap) {
        return replaceLeavesMaybe(t -> t.translateLeaf(translationMap));
    }

    @Nonnull
    @SuppressWarnings("unused")
    default Value translateLeaf(@Nonnull final Map<Value, Value> translationMap) {
        return translationMap.getOrDefault(this, this);
    }

    @Nonnull
    default <V extends Value> V narrow(@Nonnull Class<V> narrowedClass) {
        return narrowedClass.cast(this);
    }

    @Nonnull
    @Override
    default boolean semanticEquals(@Nullable final Object other,
                                   @Nonnull final AliasMap aliasMap) {
        if (other == null) {
            return false;
        }

        if (this == other) {
            return true;
        }

        if (!(other instanceof Value)) {
            return false;
        }

        final Value otherAndOrPred = (Value)other;
        if (!equalsWithoutChildren(otherAndOrPred, aliasMap)) {
            return false;
        }

        final Iterator<? extends Value> children = getChildren().iterator();
        final Iterator<? extends Value> otherChildren = otherAndOrPred.getChildren().iterator();

        while (children.hasNext()) {
            if (!otherChildren.hasNext()) {
                return false;
            }

            if (!children.next().semanticEquals(otherChildren.next(), aliasMap)) {
                return false;
            }
        }

        return !otherChildren.hasNext();
    }

    @SuppressWarnings("unused")
    default boolean equalsWithoutChildren(@Nonnull final Value other,
                                          @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        return other.getClass() == getClass();
    }

    static List<? extends Value> fromKeyExpressions(@Nonnull final Collection<? extends KeyExpression> expressions, @Nonnull final CorrelationIdentifier alias) {
        return expressions
                .stream()
                .map(keyExpression -> new ScalarTranslationVisitor(keyExpression).toResultValue(alias))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * A scalar value type that cannot be evaluated.
     */
    @API(API.Status.EXPERIMENTAL)
    interface CompileTimeValue extends Value {
        @Nullable
        @Override
        default <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                                @Nonnull final EvaluationContext context,
                                                @Nullable FDBRecord<M> record,
                                                @Nullable M message) {
            throw new RecordCoreException("value is compile-time only and cannot be evaluated");
        }
    }

    /**
     * A scalar value type that can only fetched from an index, that is the value cannot be fetched from the base record
     * nor can it be computed "on-the-fly".
     */
    @API(API.Status.EXPERIMENTAL)
    interface IndexOnlyValue extends Value {
        @Nullable
        @Override
        default <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                                @Nonnull final EvaluationContext context,
                                                @Nullable FDBRecord<M> record,
                                                @Nullable M message) {
            throw new RecordCoreException("value is index-only and cannot be evaluated");
        }
    }
}

/*
 * Value.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.Narrowable;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.UsesValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractValueRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueSimplificationPerPartRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.PullUpValueRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.ValueSimplificationRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
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

/**
 * A scalar value type.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public interface Value extends Correlated<Value>, TreeLike<Value>, UsesValueEquivalence<Value>, PlanHashable, Typed, Narrowable<Value>, PlanSerializable {

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
     * into a single structured type such as a {@link Type.Record}.
     * <br>
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
     * Returns a human-friendly textual representation of this {@link Value}.
     *
     * @param formatter The formatter used to format the textual representation.
     * @return a human-friendly textual representation of this {@link Value}.
     */
    @Nonnull
    default String explain(@Nonnull final Formatter formatter) {
        throw new UnsupportedOperationException("object of class " + this.getClass().getSimpleName() + " does not override explain");
    }

    /**
     * Checks whether this {@link Value} is compile-time constant.
     *
     * @return {@code true} if {@link Value} is compile-time constant, otherwise {@code false}.
     */
    default boolean isConstant() {
        return getCorrelatedTo().isEmpty()
                && preOrderStream().filter(NondeterministicValue.class::isInstance).findAny().isEmpty();
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
        return eval(null, context);
    }

    @Nullable
    <M extends Message> Object eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context);

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
     * Method to create a {@link PredicateWithValueAndRanges} placeholder that is based on this value. A placeholder is also a {@link QueryPredicate}
     * that is used solely for matching query predicates against.
     * @param parameterAlias alias to uniquely identify the parameter in the
     *        {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate} this placeholder will be a part of.
     * @return a new {@link PredicateWithValueAndRanges} that has {@code parameterAlias} alias.
     */
    @Nonnull
    default Placeholder asPlaceholder(@Nonnull final CorrelationIdentifier parameterAlias) {
        return Placeholder.newInstance(this, parameterAlias);
    }

    /**
     * Method to derive if this value is functionally dependent on another value. In order to produce a meaningful
     * result that {@code otherValue} and this value must parts of the result values of the same
     * {@link RelationalExpression}.
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

        return preOrderStream().flatMap(value -> value instanceof QuantifiedValue ? Stream.of((QuantifiedValue)value) : Stream.empty())
                .allMatch(quantifiedValue -> quantifiedValue.isFunctionallyDependentOn(otherValue));
    }

    @Nonnull
    Set<CorrelationIdentifier> getCorrelatedToWithoutChildren();

    @Nonnull
    @Override
    default Value rebase(@Nonnull final AliasMap aliasMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(aliasMap));
    }

    @Nonnull
    default Value translateCorrelationsAndSimplify(@Nonnull final TranslationMap translationMap) {
        final var newValue = translateCorrelations(translationMap);
        return newValue.simplify(AliasMap.emptyMap(), newValue.getCorrelatedTo());
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    default Value translateCorrelations(@Nonnull final TranslationMap translationMap) {
        return replaceLeavesMaybe(value -> {
            if (value instanceof LeafValue) {
                final var leafValue = (LeafValue)value;
                final var correlatedTo = value.getCorrelatedTo();
                if (correlatedTo.isEmpty()) {
                    return leafValue;
                }

                Verify.verify(correlatedTo.size() == 1);
                final var sourceAlias = Iterables.getOnlyElement(correlatedTo);
                return translationMap.containsSourceAlias(sourceAlias)
                       ? translationMap.applyTranslationFunction(sourceAlias, leafValue)
                       : leafValue;
            }
            Verify.verify(value.getCorrelatedTo().isEmpty());
            return value;
        }, false).orElseThrow(() -> new RecordCoreException("unable to map tree"));
    }

    @Nonnull
    default <V extends Value> V narrow(@Nonnull Class<V> narrowedClass) {
        return narrowedClass.cast(this);
    }

    /**
     * Method to compute the hash code of this value without considering the children of this {@link TreeLike}.
     * @return a hash code that similar to the regular {@link Object#hashCode()} computes a hash code, but does not
     *         incorporate the children of this value
     */
    int hashCodeWithoutChildren();

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
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

        return semanticEquals(other, ValueEquivalence.fromAliasMap(aliasMap)).isTrue();
    }

    /**
     * Overriding method of {@link UsesValueEquivalence#semanticEquals(Object, ValueEquivalence)} that attempts to
     * assert equivalence of {@code this} and {@code other} using the {@link ValueEquivalence} that was passed in.
     * @param other the other object to compare this object to
     * @param valueEquivalence the value equivalence
     * @return a boolean monad {@link BooleanWithConstraint} that is either effectively {@code false} or {@code true}
     *         under the assumption that a contained query plan constraint is satisfied
     */
    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    default BooleanWithConstraint semanticEquals(@Nullable final Object other,
                                                 @Nonnull final ValueEquivalence valueEquivalence) {
        if (this == other) {
            return BooleanWithConstraint.alwaysTrue();
        }

        if (!(other instanceof Value)) {
            return BooleanWithConstraint.falseValue();
        }

        final var thisOther = semanticEqualsTyped((Value)other, valueEquivalence);

        Debugger.sanityCheck(() -> {
            final var inverseValueEquivalenceMaybe = valueEquivalence.inverseMaybe();
            Verify.verify(inverseValueEquivalenceMaybe.isPresent());
            final var otherThis =
                    ((Value)other).semanticEqualsTyped(this, inverseValueEquivalenceMaybe.get());
            Verify.verify(thisOther.isTrue() == otherThis.isTrue());
        });

        if (thisOther.isFalse()) {
            //
            // By the looks of it, otherValue is not equal to this value. However, maybe it's already in the
            // valueEquivalence.
            //
            return valueEquivalence.isDefinedEqual(this, (Value)other);
        }

        return thisOther;
    }

    @Nonnull
    @Override
    default BooleanWithConstraint semanticEqualsTyped(@Nonnull final Value other,
                                                      @Nonnull final ValueEquivalence valueEquivalence) {
        final var equalsWithoutChildren = equalsWithoutChildren(other);
        if (equalsWithoutChildren.isFalse()) {
            return BooleanWithConstraint.falseValue();
        }

        var constraint = equalsWithoutChildren;
        final Iterator<? extends Value> children = getChildren().iterator();
        final Iterator<? extends Value> otherChildren = other.getChildren().iterator();

        while (children.hasNext()) {
            if (!otherChildren.hasNext()) {
                return BooleanWithConstraint.falseValue();
            }

            final var isChildEquals =
                    children.next().semanticEquals(otherChildren.next(), valueEquivalence);
            if (isChildEquals.isFalse()) {
                return BooleanWithConstraint.falseValue();
            }

            constraint = constraint.composeWithOther(isChildEquals);
        }

        if (otherChildren.hasNext()) {
            // otherValue has more children, it cannot be equivalent
            return BooleanWithConstraint.falseValue();
        }

        return constraint;
    }

    @Nonnull
    @SuppressWarnings({"unused", "PMD.CompareObjectsWithEquals"})
    default BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        if (this == other) {
            return BooleanWithConstraint.alwaysTrue();
        }

        return other.getClass() == getClass() ? BooleanWithConstraint.alwaysTrue() : BooleanWithConstraint.falseValue();
    }

    default boolean canResultInType(@Nonnull final Type type) {
        return false;
    }

    @Nonnull
    default Value with(@Nonnull final Type type) {
        throw new RecordCoreException("cannot promote to type"); // TODO coerce type here
    }

    @Nonnull
    default Optional<Value> promoteToTypeMaybe(@Nonnull final Type type) {
        if (canResultInType(type)) {
            return Optional.of(with(type));
        }
        return Optional.empty();
    }

    @Nonnull
    PValue toValueProto(@Nonnull PlanSerializationContext serializationContext);

    @Nonnull
    static Value fromValueProto(@Nonnull final PlanSerializationContext serializationContext,
                                @Nonnull final PValue valueProto) {
        return (Value)PlanSerialization.dispatchFromProtoContainer(serializationContext, valueProto);
    }

    static List<Value> fromKeyExpressions(@Nonnull final Collection<? extends KeyExpression> expressions, @Nonnull final Quantifier quantifier) {
        return fromKeyExpressions(expressions, quantifier.getAlias(), quantifier.getFlowedObjectType());
    }

    static List<Value> fromKeyExpressions(@Nonnull final Collection<? extends KeyExpression> expressions, @Nonnull final CorrelationIdentifier alias, @Nonnull final Type inputType) {
        return expressions
                .stream()
                .map(keyExpression -> new ScalarTranslationVisitor(keyExpression).toResultValue(alias, inputType))
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
                                                @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("value is compile-time only and cannot be evaluated");
        }
    }

    /**
     * A marker interface for {@link Value}s that can be used in the context of range construction, it must be evaluable
     * without being bound to an {@link FDBRecordStoreBase}.
     * See {@link com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints}.
     */
    @API(API.Status.EXPERIMENTAL)
    interface RangeMatchableValue extends Value {
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
                                                @Nonnull final EvaluationContext context) {
            throw new RecordCoreException("value is index-only and cannot be evaluated");
        }
    }

    /**
     * Tag interface for marking a {@link Value} that is non-deterministic, i.e. each time we call
     * {@link Value#eval(FDBRecordStoreBase, EvaluationContext)} it might produce a different
     * result.
     */
    @API(API.Status.EXPERIMENTAL)
    interface NondeterministicValue extends Value {}

    /**
     * Method to simplify this value using a rule set passed in.
     * @param ruleSet a rule set
     * @param aliasMap and alias map of equalities
     * @param constantAliases a set of aliases that are considered to be constant
     * @return a new (simplified) value
     */
    @Nonnull
    default Value simplify(@Nonnull final AbstractValueRuleSet<Value, ValueSimplificationRuleCall> ruleSet,
                           @Nonnull final AliasMap aliasMap,
                           @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return Simplification.simplify(this, aliasMap, constantAliases, ruleSet);
    }

    /**
     * Method to simplify this value using the default simplification rule set.
     * @param aliasMap and alias map of equalities
     * @param constantAliases a set of aliases that are considered to be constant
     * @return a new (simplified) value
     */
    @Nonnull
    default Value simplify(@Nonnull final AliasMap aliasMap,
                           @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return Simplification.simplify(this, aliasMap, constantAliases, DefaultValueSimplificationRuleSet.ofSimplificationRules());
    }

    /**
     * Method to pull up a list of values through this value. The logic employed by this method heavily
     * relies on value simplification techniques. The goal of pulling up a value {@code v} through some other value
     * (this value) is to express {@code v} as if applied on top of this value. For instance, if this method is called
     * for some value {@code _.x} on {@code this} value {@code (_.x as a, _.y as b)}, the result is {@code _.a}.
     * This method supports to pull up as list of values together, as pulling up many values at once is more efficient
     * than to separately pulling up the individual elements of the list.
     * @param toBePulledUpValues a list of {@link Value}s to be pulled up through {@code this}
     * @param aliasMap an alias map of equalities
     * @param constantAliases a set of aliases that are considered to be constant
     * @param upperBaseAlias an alias to be used as <em>current</em> alias
     * @return a map from {@link Value} to {@link Value} that related the values that the called passed in with the
     *         resulting values of the pull-up logic
     */
    @Nonnull
    default Map<Value, Value> pullUp(@Nonnull final Iterable<? extends Value> toBePulledUpValues,
                                     @Nonnull final AliasMap aliasMap,
                                     @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                     @Nonnull final CorrelationIdentifier upperBaseAlias) {
        final var resultPair =
                Simplification.compute(this, toBePulledUpValues, aliasMap, constantAliases, PullUpValueRuleSet.ofPullUpValueRules());
        if (resultPair == null) {
            return ImmutableMap.of();
        }

        final var matchedValuesMap = resultPair.getRight();
        final var resultsMap = new LinkedIdentityMap<Value, Value>();
        for (final var toBePulledUpValue : toBePulledUpValues) {
            final var compensation = matchedValuesMap.get(toBePulledUpValue);
            if (compensation != null) {
                resultsMap.put(toBePulledUpValue,
                        compensation.compensate(upperBaseAlias,
                                QuantifiedObjectValue.of(upperBaseAlias, this.getResultType())));
            }
        }

        return resultsMap;
    }

    /**
     * Method to push down a list of values through this value. The logic employed by this method heavily
     * relies on value simplification techniques. The goal of pushing down a value {@code v} through some other value
     * (this value) is to express {@code v} in terms of aliases used by {@code this}. For instance, if this method is
     * called for some value {@code _.a} on {@code this} value {@code (_.x as a, _.y as b)}, the result is {@code _.x}.
     * This method supports to push down as list of values together, as pushing down many values at once is more efficient
     * than to separately pushing down the individual elements of the list.
     * @param toBePushedDownValues a list of {@link Value}s to be pushed down through {@code this}
     * @param simplificationRuleSet a rule set to be used for simplification while pushing down values
     * @param aliasMap an alias map of equalities
     * @param constantAliases a set of aliases that are considered to be constant
     * @param upperBaseAlias an alias to be treated as <em>current</em> alias
     * @return a map from {@link Value} to {@link Value} that related the values that the called passed in with the
     *         resulting values of the pull-up logic
     */
    @Nonnull
    default List<Value> pushDown(@Nonnull final Iterable<? extends Value> toBePushedDownValues,
                                 @Nonnull final AbstractValueRuleSet<Value, ValueSimplificationRuleCall> simplificationRuleSet,
                                 @Nonnull final AliasMap aliasMap,
                                 @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                 @Nonnull final CorrelationIdentifier upperBaseAlias) {
        return Streams.stream(toBePushedDownValues)
                .map(toBePushedDownValue ->
                        toBePushedDownValue.replaceLeavesMaybe(value -> {
                            if (value instanceof QuantifiedObjectValue && ((QuantifiedObjectValue)value).getAlias().equals(upperBaseAlias)) {
                                return this;
                            }
                            return value;
                        }))
                .map(valueOptional -> valueOptional.orElseThrow(() -> new RecordCoreException("unexpected empty optional")))
                .map(composedValue -> composedValue.simplify(simplificationRuleSet, aliasMap, constantAliases))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Method to simplify the current value using the specific rule set for orderings. When values pertaining to
     * orderings are simplified we often can simplify more aggressively knowing that this value is only used to
     * express ordering parts.
     * <br>
     * For instance, a value representing {@code ((_.a, (_.b, _.c)) as x, d).x} can be
     * simplified to {@code (_.a, (_.b, _.c))} with regular default simplification rules, but it can further be
     * simplified to {@code (_.a, _.b, _.c)} as the additional level of nesting is not important for orderings.
     * The record constructor {@code (_.a, _.b, _.c)} is then deconstructed a list consisting of the values
     * {@code [_.a, _.b, _.c]} is returned.
     * @param aliasMap an alias map of equalities
     * @param constantAliases a set of aliases that are considered to be constant
     * @return a list of values that is the deconstructed simplified representation of this value using
     *         ordering simplification rules
     */
    @Nonnull
    default List<Value> simplifyOrderingValue(@Nonnull final AliasMap aliasMap, @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        final var simplifiedOrderingValue =
                Simplification.simplify(this, aliasMap, constantAliases, OrderingValueSimplificationRuleSet.ofOrderingSimplificationRules());

        if (simplifiedOrderingValue instanceof RecordConstructorValue) {
            return Streams.stream(simplifiedOrderingValue.getChildren())
                    .map(partValue -> Simplification.simplify(partValue, aliasMap, constantAliases, OrderingValueSimplificationPerPartRuleSet.ofOrderingSimplificationPerPartRules()))
                    .collect(ImmutableList.toImmutableList());
        } else {
            return ImmutableList.of(Simplification.simplify(simplifiedOrderingValue, aliasMap, constantAliases, OrderingValueSimplificationPerPartRuleSet.ofOrderingSimplificationPerPartRules()));
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    default BooleanWithConstraint subsumedBy(@Nullable final Value other, @Nonnull final ValueEquivalence valueEquivalence) {
        // delegate to semanticEquals()
        return semanticEquals(other, valueEquivalence);
    }
}

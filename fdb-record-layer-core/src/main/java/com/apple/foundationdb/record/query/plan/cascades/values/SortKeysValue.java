/*
 * SortKeysValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PSortKeysValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The sort keys of the in-call {@code ORDER BY} clause of an order-sensitive aggregate function, bundled into a single
 * {@link Value}.
 *
 * <p>This is a <em>pseudo-value</em>; it is never evaluated. It exists so that an order-sensitive aggregate can carry
 * its {@code ORDER BY} clause as one child and receive it as one call-site argument. Representing them as a child has
 * the benefit that the sort expressions are automatically rebased, translated and simplified along with the aggregate
 * that holds them.
 *
 * <p>The requirement expressed by the {@code ORDER BY} clause is served by the planner. The value is “consumed” by
 * {@code GroupByExpression}, which turns the sort keys into an ordering it requests of its input.
 */
@API(API.Status.EXPERIMENTAL)
public class SortKeysValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sort-Keys-Value");

    /**
     * The sort keys, in declared order. This list is never empty. (An aggregate without an in-call {@code ORDER BY}
     * clause holds no {@code SortKeysValue} at all.)
     */
    @Nonnull
    private final List<SortKey> sortKeys;

    /**
     * The sort orders of {@link #sortKeys}. These are kept separately because they, unlike the sort key values, are not
     * children and therefore have to take part in the “without children” hashing and equality.
     */
    @Nonnull
    private final List<RequestedSortOrder> sortOrders;

    public SortKeysValue(@Nonnull final List<SortKey> sortKeys) {
        Verify.verify(!sortKeys.isEmpty(), "a sort keys value needs at least one sort key");
        this.sortKeys = ImmutableList.copyOf(sortKeys);
        this.sortOrders = this.sortKeys.stream().map(SortKey::getSortOrder).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public List<SortKey> getSortKeys() {
        return sortKeys;
    }

    /**
     * Returns the single in-call {@code ORDER BY} requirement imposed by the given value, typically the aggregate value
     * of a {@code GroupByExpression}.
     *
     * @param value the value to collect the in-call sort keys from
     * @return the sort keys all ordered aggregates in {@code value} agree on, or {@code Optional.empty()} if none of
     *         them carries an in-call {@code ORDER BY} clause
     */
    @Nonnull
    public static Optional<SortKeysValue> commonSortKeysOf(@Nonnull final Value value) {
        final List<SortKeysValue> distinctSortKeys = distinctSortKeysOf(Stream.of(value));
        // If no aggregate carries an `ORDER BY`, they do not impose any requirement.
        if (distinctSortKeys.isEmpty()) {
            return Optional.empty();
        }
        // Sanity-check that there is no conflict. The SQL frontend is supposed to reject such aggregates.
        if (distinctSortKeys.size() > 1) {
            throw new RecordCoreException("conflicting in-call ORDER BY clauses among the aggregates of a group");
        }
        return Optional.of(Iterables.getOnlyElement(distinctSortKeys));
    }

    /**
     * Returns the distinct in-call {@code ORDER BY} requirements imposed by the given values. More than one of them is
     * a conflict, which callers report in a suitable manner.
     *
     * @param values the values to collect the in-call sort keys from, typically the aggregate values of one group-by
     * @return the distinct sort keys of the ordered aggregates in {@code values}, possibly empty
     */
    @Nonnull
    public static List<SortKeysValue> distinctSortKeysOf(@Nonnull final Stream<? extends Value> values) {
        return values.flatMap(Value::preOrderStream)
                .filter(SortKeysValue.class::isInstance)
                .map(SortKeysValue.class::cast)
                .distinct()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns these sort keys as ordering parts, rebased from the given alias onto {@link Quantifier#current()}. The
     * rebase is not optional: an in-call sort key is expressed over the input of the aggregate holding it, whereas
     * {@link OrderingPart} verifies that its value is correlated to nothing but {@code current()}.
     *
     * @param sourceAlias the alias the sort keys are currently expressed over
     * @return the sort keys as ordering parts, in declared order
     */
    @Nonnull
    public List<RequestedOrderingPart> toOrderingPartsOnCurrent(@Nonnull final CorrelationIdentifier sourceAlias) {
        final AliasMap toCurrent = AliasMap.ofAliases(sourceAlias, Quantifier.current());
        return sortKeys.stream()
                .map(sortKey -> new RequestedOrderingPart(sortKey.getValue().rebase(toCurrent),
                        sortKey.getSortOrder()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * {@inheritDoc}
     * <p>A sort keys value is a pseudo-value that only ever carries a planner requirement, so it has no type of its
     * own.
     */
    @Nonnull
    @Override
    public Type getResultType() {
        return new Type.Any();
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval the sort keys of an in-call ORDER BY clause");
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return sortKeys.stream().map(SortKey::getValue).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public SortKeysValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == sortKeys.size());
        final ImmutableList.Builder<SortKey> newSortKeys = ImmutableList.builder();
        for (int i = 0; i < sortKeys.size(); i++) {
            newSortKeys.add(sortKeys.get(i).withValue(Iterables.get(newChildren, i)));
        }
        return new SortKeysValue(newSortKeys.build());
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(
            @Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final ExplainTokens explainTokens = new ExplainTokens();
        for (int i = 0; i < sortKeys.size(); i++) {
            if (i > 0) {
                explainTokens.addCommaAndWhiteSpace();
            }
            explainTokens.addNested(Iterables.get(explainSuppliers, i).get().getExplainTokens())
                    .addWhitespace().addToString(sortKeys.get(i).getSortOrder().getArrowIndicator());
        }
        return ExplainTokensWithPrecedence.of(explainTokens);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, sortOrders);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren(), sortOrders);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> other instanceof SortKeysValue o && sortOrders.equals(o.sortOrders));
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
    @Override
    public PSortKeysValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PSortKeysValue.Builder builder = PSortKeysValue.newBuilder();
        for (final SortKey sortKey : sortKeys) {
            builder.addSortKeys(sortKey.toProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setSortKeysValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static SortKeysValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PSortKeysValue proto) {
        final ImmutableList.Builder<SortKey> sortKeys = ImmutableList.builder();
        for (final PSortKeysValue.PSortKey sortKeyProto : proto.getSortKeysList()) {
            sortKeys.add(SortKey.fromProto(serializationContext, sortKeyProto));
        }
        return new SortKeysValue(sortKeys.build());
    }

    /**
     * One sort key of an in-call {@code ORDER BY} clause. Comprises the expression to sort by, together with its sort
     * order. The sort key value is a child of the enclosing {@link SortKeysValue} and is therefore rebased and
     * translated along with it.
     */
    public static final class SortKey {
        @Nonnull
        private final Value value;

        @Nonnull
        private final RequestedSortOrder sortOrder;

        public SortKey(@Nonnull final Value value, @Nonnull final RequestedSortOrder sortOrder) {
            Verify.verify(sortOrder != RequestedSortOrder.ANY, "an in-call sort key needs a definite sort order");
            this.value = value;
            this.sortOrder = sortOrder;
        }

        @Nonnull
        public Value getValue() {
            return value;
        }

        @Nonnull
        public RequestedSortOrder getSortOrder() {
            return sortOrder;
        }

        @Nonnull
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public SortKey withValue(@Nonnull final Value newValue) {
            return newValue == value ? this : new SortKey(newValue, sortOrder);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof SortKey o && sortOrder == o.sortOrder && value.equals(o.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, sortOrder);
        }

        @Override
        public String toString() {
            return value + sortOrder.getArrowIndicator();
        }

        @Nonnull
        PSortKeysValue.PSortKey toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PSortKeysValue.PSortKey.newBuilder()
                    .setValue(value.toValueProto(serializationContext))
                    .setSortOrder(toProto(sortOrder))
                    .build();
        }

        @Nonnull
        static SortKey fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PSortKeysValue.PSortKey proto) {
            return new SortKey(Value.fromValueProto(serializationContext, Objects.requireNonNull(proto.getValue())),
                    fromProto(proto.getSortOrder()));
        }

        @Nonnull
        private static PSortKeysValue.PSortOrder toProto(@Nonnull final RequestedSortOrder sortOrder) {
            return switch (sortOrder) {
                case ASCENDING -> PSortKeysValue.PSortOrder.ASCENDING;
                case DESCENDING -> PSortKeysValue.PSortOrder.DESCENDING;
                case ASCENDING_NULLS_LAST -> PSortKeysValue.PSortOrder.ASCENDING_NULLS_LAST;
                case DESCENDING_NULLS_FIRST -> PSortKeysValue.PSortOrder.DESCENDING_NULLS_FIRST;
                default -> throw new RecordCoreException("unexpected sort order")
                        .addLogInfo(LogMessageKeys.VALUE, sortOrder);
            };
        }

        @Nonnull
        private static RequestedSortOrder fromProto(@Nonnull final PSortKeysValue.PSortOrder proto) {
            return switch (proto) {
                case ASCENDING -> RequestedSortOrder.ASCENDING;
                case DESCENDING -> RequestedSortOrder.DESCENDING;
                case ASCENDING_NULLS_LAST -> RequestedSortOrder.ASCENDING_NULLS_LAST;
                case DESCENDING_NULLS_FIRST -> RequestedSortOrder.DESCENDING_NULLS_FIRST;
                default -> throw new RecordCoreException("unexpected sort order")
                        .addLogInfo(LogMessageKeys.VALUE, proto);
            };
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PSortKeysValue, SortKeysValue> {
        @Nonnull
        @Override
        public Class<PSortKeysValue> getProtoMessageClass() {
            return PSortKeysValue.class;
        }

        @Nonnull
        @Override
        public SortKeysValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PSortKeysValue proto) {
            return SortKeysValue.fromProto(serializationContext, proto);
        }
    }
}

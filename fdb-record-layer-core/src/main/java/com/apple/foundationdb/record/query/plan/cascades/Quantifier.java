/*
 * Quantifier.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PPhysicalQuantifier;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Models the concept of quantification.
 * 
 * <p>
 * A quantifier describes the data flow between the output of one {@link RelationalExpression} {@code R} and the
 * consumption of that data by another {@code RelationalExpression} {@code S}. {@code S} is said to own the quantifier,
 * while the quantifier is said to range over {@code R}. Quantifiers come in very few but very distinct flavors.
 * All flavors are implemented by static inner final classes in order to emulate a sealed trait.
 * </p>
 *
 * <p>
 * Quantifiers separate what it means to be producing versus consuming items. The expression a quantifier ranges over
 * produces records, the quantifier flows information (in a manner determined by the flavor) that is consumed by the expression
 * containing or owning the quantifier. That expression can consume the data in a way independent of how the data was
 * produced in the first place.
 * </p>
 *
 * <p>
 * A quantifier works closely with the expression that owns it. Depending on the semantics of the owning expression
 * it becomes possible to model correlations. For example, in a logical join expression the quantifier can provide a binding
 * of the item being currently consumed by the join's outer to other (inner) parts of the data flow that are also rooted
 * at the owning (join) expression.
 * </p>
 */
@SuppressWarnings("unused")
@API(API.Status.EXPERIMENTAL)
public abstract class Quantifier implements Correlated<Quantifier> {
    /**
     * A (fake alias) that references the "current" record.
     */
    @Nonnull
    private static final UUID CURRENT = UUID.randomUUID();

    /**
     * Constant quantifier representing a reference to a constant box of literals.
     */
    @Nonnull
    private static final String CONSTANT = "CONSTANT";

    /**
     * The alias (some identification) for this quantifier.
     */
    @Nonnull
    private final CorrelationIdentifier alias;

    /**
     * As a quantifier is immutable, the correlated set can be computed lazily and then cached. This supplier
     * represents that cached set.
     */
    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToSupplier;

    /**
     * As a quantifier is immutable, the columns that flow along the quantifier can be lazily computed.
     */
    @Nonnull
    private final Supplier<List<Column<? extends FieldValue>>> flowedColumnsSupplier;

    /**
     * As a quantifier is immutable, the values that flow along the quantifier can be lazily computed.
     */
    @Nonnull
    private final Supplier<List<? extends FieldValue>> flowedValuesSupplier;

    /**
     * Builder class for quantifiers.
     * @param <Q> quantifier type
     * @param <B> builder type
     */
    public abstract static class Builder<Q extends Quantifier, B extends Builder<Q, B>> {
        @Nullable
        protected CorrelationIdentifier alias;

        @Nonnull
        public B from(final Q quantifier) {
            return withAlias(quantifier.getAlias());
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        public B withAlias(final CorrelationIdentifier alias) {
            this.alias = alias;
            return (B)this;
        }

        @Nonnull
        public abstract Quantifier build(@Nonnull Reference rangesOver);
    }

    /**
     * A quantifier that conceptually flows one item at a time from the expression it ranges over to
     * the owning expression.
     */
    @SuppressWarnings("squid:S2160") // sonarqube thinks .equals() and hashCode() should be overwritten which is not necessary
    public static final class ForEach extends Quantifier {
        @Nonnull private final Reference rangesOver;

        /**
         * Builder subclass to build for-each quantifiers.
         */
        public static class ForEachBuilder extends Builder<ForEach, ForEachBuilder> {
            @Nonnull
            @Override
            public ForEach build(@Nonnull final Reference rangesOver) {
                return new ForEach(alias == null ? Quantifier.uniqueID() : alias,
                        rangesOver);
            }
        }

        private ForEach(@Nonnull final CorrelationIdentifier alias,
                        @Nonnull final Reference rangesOver) {
            super(alias);
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public Reference getRangesOver() {
            return rangesOver;
        }

        @Override
        @Nonnull
        public Builder<? extends Quantifier, ? extends Builder<?, ?>> toBuilder() {
            return new ForEachBuilder()
                    .from(this);
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "ƒ";
        }

        @Override
        @Nonnull
        public ForEach overNewReference(@Nonnull final Reference reference) {
            return Quantifier.forEachBuilder()
                    .from(this)
                    .build(reference);
        }

        @Nonnull
        @Override
        public List<Column<? extends FieldValue>> computeFlowedColumns() {
            return pullUpResultColumns(getFlowedObjectType(), getAlias());
        }
    }

    /**
     * Create a builder for a for-each quantifier containing relational expressions.
     * @return a for-each quantifier builder
     */
    @Nonnull
    public static ForEach.ForEachBuilder forEachBuilder() {
        return new ForEach.ForEachBuilder();
    }

    /**
     * Shorthand to create a for-each quantifier ranging over a reference.
     * @param reference the reference
     * @return a new for-each quantifier ranging over {@code reference}
     */
    @Nonnull
    public static ForEach forEach(@Nonnull final Reference reference) {
        return forEachBuilder()
                .build(reference);
    }

    /**
     * Shorthand to create a for-each quantifier ranging over a reference using a given alias.
     * @param reference the reference
     * @param alias the alias to be used
     * @return a new for-each quantifier ranging over {@code reference}
     */
    @Nonnull
    public static ForEach forEach(@Nonnull final Reference reference,
                                  @Nonnull final CorrelationIdentifier alias) {
        return forEachBuilder()
                .withAlias(alias)
                .build(reference);
    }

    /**
     * A quantifier that conceptually flows exactly one item containing a boolean to the owning
     * expression indicating whether the sub-graph that the quantifier ranges over, produced a non-empty or an empty
     * result. When the semantics of these quantifiers are realized in an execution strategy that strategy should
     * facilitate a boolean "short-circuit" mechanism as the result will be {@code true} as soon as the sub-graph produces
     * the first item.
     */
    @SuppressWarnings("squid:S2160") // sonarqube thinks .equals() and hashCode() should be overwritten which is not necessary
    public static final class Existential extends Quantifier {
        @Nonnull
        private final Reference rangesOver;

        /**
         * Builder subclass for existential quantifiers.
         */
        public static class ExistentialBuilder extends Builder<Existential, ExistentialBuilder> {
            @Override
            @Nonnull
            public Existential build(@Nonnull final Reference rangesOver) {
                return new Existential(alias == null ? Quantifier.uniqueID() : alias,
                        rangesOver);
            }
        }

        private Existential(@Nonnull final CorrelationIdentifier alias,
                            @Nonnull final Reference rangesOver) {
            super(alias);
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public Reference getRangesOver() {
            return rangesOver;
        }

        @Override
        @Nonnull
        public Builder<? extends Quantifier, ? extends Builder<?, ?>> toBuilder() {
            return new Existential.ExistentialBuilder()
                    .from(this);
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "∃";
        }

        @Override
        @Nonnull
        public Existential overNewReference(@Nonnull final Reference reference) {
            return Quantifier.existentialBuilder()
                    .from(this)
                    .build(reference);
        }

        @Nonnull
        @Override
        public List<Column<? extends FieldValue>> computeFlowedColumns() {
            throw new IllegalStateException("should not be called");
        }
    }

    /**
     * Create a builder for an existential quantifier containing relational
     * expressions.
     * @return an existential quantifier builder
     */
    @Nonnull
    public static Existential.ExistentialBuilder existentialBuilder() {
        return new Existential.ExistentialBuilder();
    }

    /**
     * Shorthand to create an existential quantifier ranging over a reference.
     * @param reference the reference
     * @return a new existential quantifier ranging over {@code reference}
     */
    @Nonnull
    public static Existential existential(@Nonnull final Reference reference) {
        return existentialBuilder()
                .build(reference);
    }

    /**
     * Shorthand to create an existential quantifier ranging over a reference using a given alias.
     * @param reference the reference
     * @param alias the alias to be used
     * @return a new existential quantifier ranging over {@code reference}
     */
    @Nonnull
    public static Existential existential(@Nonnull final Reference reference,
                                          @Nonnull final CorrelationIdentifier alias) {
        return existentialBuilder()
                .withAlias(alias)
                .build(reference);
    }

    /**
     * Physical quantifier. This kind of quantifier is the conduit between two {@link RecordQueryPlan}s. It does
     * not have any associated semantics; all semantics and execution details must be subsumed
     * by the query plans themselves.
     */
    @SuppressWarnings("squid:S2160") // sonarqube thinks .equals() and hashCode() should be overwritten which is not necessary
    public static final class Physical extends Quantifier implements PlanSerializable {
        @Nonnull private final Reference rangesOver;

        /**
         * Builder subclass for physical quantifiers.
         */
        public static class PhysicalBuilder extends Builder<Physical, PhysicalBuilder> {
            @Nonnull
            @Override
            public Physical build(@Nonnull final Reference rangesOver) {
                return new Physical(alias == null ? Quantifier.uniqueID() : alias, rangesOver);
            }

            /**
             * Build a new physical quantifier from a for-each quantifier with the same alias.
             * Often times a for-each quantifier needs to "turn" into a physical quantifier e.g. when a logical
             * operator is implemented by a physical one.
             * @param quantifier for each quantifier to morph from
             * @return the new physical quantifier
             */
            @Nonnull
            public PhysicalBuilder morphFrom(@Nonnull final ForEach quantifier) {
                return withAlias(quantifier.getAlias());
            }
        }

        private Physical(@Nonnull final CorrelationIdentifier alias,
                         @Nonnull final Reference rangesOver) {
            super(alias);
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public Reference getRangesOver() {
            return rangesOver;
        }

        @Nonnull
        public RecordQueryPlan getRangesOverPlan() {
            return (RecordQueryPlan)getRangesOver().get();
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "𝓅";
        }

        @Override
        @Nonnull
        public Builder<? extends Quantifier, ? extends Builder<?, ?>> toBuilder() {
            return new Physical.PhysicalBuilder()
                    .from(this);
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(final Object other) {
            return structuralEquals(other);
        }

        @Override
        public int hashCode() {
            return structuralHashCode();
        }

        public boolean structuralEquals(@Nullable final Object other) {
            return structuralEquals(other, AliasMap.emptyMap());
        }

        public boolean structuralEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
            if (!(other instanceof Physical)) {
                return false;
            }
            return getRangesOverPlan().structuralEquals(((Physical)other).getRangesOverPlan(), equivalenceMap);
        }

        public int structuralHashCode() {
            return getRangesOverPlan().structuralHashCode();
        }

        @Override
        @Nonnull
        public Physical overNewReference(@Nonnull final Reference reference) {
            return Quantifier.physicalBuilder()
                    .from(this)
                    .build(reference);
        }

        @Nonnull
        @Override
        public List<Column<? extends FieldValue>> computeFlowedColumns() {
            return pullUpResultColumns(getFlowedObjectType(), getAlias());
        }

        @Nonnull
        @Override
        public PPhysicalQuantifier toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PPhysicalQuantifier.Builder builder = PPhysicalQuantifier.newBuilder()
                    .setAlias(getAlias().getId());
            final LinkedIdentitySet<RelationalExpression> members = getRangesOver().getMembers();
            for (final RelationalExpression member : members) {
                Verify.verify(member instanceof RecordQueryPlan);
                builder.addPlanReferences(serializationContext.toPlanReferenceProto((RecordQueryPlan)member));
            }
            return builder.build();
        }

        @Nonnull
        public static Physical fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PPhysicalQuantifier physicalQuantifierProto) {
            final ImmutableList.Builder<RelationalExpression> membersBuilder = ImmutableList.builder();
            for (int i = 0; i < physicalQuantifierProto.getPlanReferencesCount(); i ++) {
                membersBuilder.add(serializationContext.fromPlanReferenceProto(physicalQuantifierProto.getPlanReferences(i)));
            }
            final Reference reference = Reference.from(membersBuilder.build());
            return physicalBuilder().withAlias(CorrelationIdentifier.of(Objects.requireNonNull(physicalQuantifierProto.getAlias())))
                    .build(reference);
        }
    }

    /**
     * Create a builder for a physical quantifier containing record query plans.
     * @return a physical quantifier builder
     */
    public static Physical.PhysicalBuilder physicalBuilder() {
        return new Physical.PhysicalBuilder();
    }

    public static Physical physical(@Nonnull final Reference reference) {
        return physicalBuilder()
                .build(reference);
    }

    @Nonnull
    public static Physical physical(@Nonnull final Reference reference,
                                    @Nonnull final CorrelationIdentifier alias) {
        return physicalBuilder()
                .withAlias(alias)
                .build(reference);
    }

    protected Quantifier(@Nonnull final CorrelationIdentifier alias) {
        this.alias = alias;
        this.correlatedToSupplier = Suppliers.memoize(() -> getRangesOver().getCorrelatedTo());
        this.flowedColumnsSupplier = Suppliers.memoize(this::computeFlowedColumns);
        this.flowedValuesSupplier = Suppliers.memoize(this::computeFlowedValues);
        // Call debugger hook for this new quantifier.
        Debugger.registerQuantifier(this);
    }

    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    public abstract Quantifier.Builder<? extends Quantifier, ? extends Quantifier.Builder<?, ?>> toBuilder();

    /**
     * Return the reference that the quantifier ranges over.
     * @return {@link Reference} this quantifier ranges over
     */
    @Nonnull
    public abstract Reference getRangesOver();

    /**
     * Return a shorthand string for the quantifier. As a quantifier's semantics is usually quite subtle and should
     * not distract from expressions. For example, when a data flow is visualized the returned string should be <em>short</em>.
     * @return a short string representing the quantifier.
     */
    @Nonnull
    public abstract String getShorthand();

    /**
     * Allow the computation of {@link ExpressionProperty}s across the quantifiers in the data flow graph.
     * @param visitor the planner property that is being computed
     * @param <U> the type of the property being computed
     * @return the property
     */
    @Nullable
    public <U> U acceptPropertyVisitor(@Nonnull ExpressionProperty<U> visitor) {
        if (visitor.shouldVisit(this)) {
            return visitor.evaluateAtQuantifier(this, getRangesOver().acceptPropertyVisitor(visitor));
        }
        return null;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (!equalsOnKind(other)) {
            return false;
        }
        final Quantifier that = (Quantifier)Objects.requireNonNull(other);
        return getRangesOver().semanticEquals(that.getRangesOver(), aliasMap);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsOnKind(final Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(getShorthand(), getRangesOver().semanticHashCode());
    }

    @Override
    @Nonnull
    public String toString() {
        return getShorthand() + "(" + getAlias() + ") -> {" +
               getCorrelatedTo().stream().map(CorrelationIdentifier::toString).collect(Collectors.joining(", ")) +
               "}";
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return correlatedToSupplier.get();
    }

    /**
     * Helper to determine if anything that this quantifier ranges over is correlated to something that needs to be
     * rebased.
     * @param translationMap a map that expresses translations from correlations identifiers to correlation identifiers.
     * @return {@code true} if the graph this quantifier ranges over needs to be rebased given the translation map
     *         passed in, {@code false} otherwise
     */
    protected boolean needsRebase(@Nonnull final AliasMap translationMap) {
        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();

        // translations are usually smaller, we may want to flip this around if needed later
        return translationMap.sources()
                .stream()
                .anyMatch(correlatedTo::contains);
    }

    @Nonnull
    public <Q extends Quantifier> Q narrow(@Nonnull Class<Q> narrowedClass) {
        return narrowedClass.cast(this);
    }

    @Nonnull
    public <Q extends Quantifier> Optional<Q> narrowMaybe(@Nonnull Class<Q> narrowedClass) {
        if (narrowedClass.isInstance(this)) {
            return Optional.of(narrowedClass.cast(this));
        }
        return Optional.empty();
    }

    @Nonnull
    public List<Column<? extends FieldValue>> getFlowedColumns() {
        return flowedColumnsSupplier.get();
    }

    @Nonnull
    protected abstract List<Column<? extends FieldValue>> computeFlowedColumns();

    @Nonnull
    protected static List<Column<? extends FieldValue>> pullUpResultColumns(@Nonnull final Type type, @Nonnull CorrelationIdentifier alias) {
        final List<Field> fields;
        if (type instanceof Type.Record) {
            fields = Objects.requireNonNull(((Type.Record)type).getFields());
        } else {
            throw new IllegalStateException("quantifier does not flow records");
        }

        final var recordType = (Type.Record)type;
        final var resultBuilder = ImmutableList.<Column<? extends FieldValue>>builder();
        for (var i = 0; i < fields.size(); i++) {
            final var field = fields.get(i);
            resultBuilder.add(Column.of(field, FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(alias, recordType), Math.toIntExact(i))));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public List<? extends FieldValue> getFlowedValues() {
        return flowedValuesSupplier.get();
    }

    @Nonnull
    private List<? extends FieldValue> computeFlowedValues() {
        return getFlowedColumns()
                .stream()
                .map(Column::getValue)
                .collect(ImmutableList.toImmutableList());
    }

    public QuantifiedObjectValue getFlowedObjectValue() {
        return QuantifiedObjectValue.of(getAlias(), getFlowedObjectType());
    }

    @Nonnull
    public Type getFlowedObjectType() {
        final var resolvedTypeAcrossReference = getRangesOver().getResultType();
        Verify.verify(resolvedTypeAcrossReference.getTypeCode() == Type.TypeCode.RELATION);
        return Objects.requireNonNull(((Type.Relation)resolvedTypeAcrossReference).getInnerType());
    }

    @Nonnull
    public abstract Quantifier overNewReference(@Nonnull Reference reference);

    @Override
    @Nonnull
    public Quantifier rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Quantifier translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final Reference rangesOver = getRangesOver();
        final Reference translatedReference =
                getRangesOver().translateCorrelations(translationMap);
        return rangesOver == translatedReference
               ? this
               : overNewReference(translatedReference);
    }

    @Nonnull
    public static CorrelationIdentifier uniqueID() {
        return CorrelationIdentifier.uniqueID(Quantifier.class);
    }

    @Nonnull
    public static CorrelationIdentifier current() {
        return CorrelationIdentifier.uniqueSingletonID(CURRENT, "𝓆");
    }

    @Nonnull
    public static CorrelationIdentifier constant() {
        return CorrelationIdentifier.of(CONSTANT);
    }
}

/*
 * IndexEntryColumnValue.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PIndexEntryColumnValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.getForOrdinalPath;

/**
 * A value that reads one column of an index entry, that is, one element of either the key or the value tuple of that
 * entry, addressed by an ordinal path (dewey id). This is useful in conjunction with covering index scans, which decode
 * their index entries into partial records. This value can be used to access the underlying entry that such a partial
 * record was decoded from.
 *
 * <p>This value is not to be confused with the closely related {@link IndexEntryObjectValue}. Both values address an
 * index entry by an ordinal path. However, {@link IndexEntryObjectValue} reads the entry from the bindings of the
 * {@link EvaluationContext}, and that binding exists only while an entry is being decoded into a partial record. This
 * value, by contrast, reads the entry from the record that flows from its correlated quantifier (as that record keeps
 * a reference to the index entry it was decoded from). This allows us to evaluate this value at any position above a
 * covering index scan, provided that every operator in between flows that record on unchanged. (An operator that builds
 * a new result, such as a map, drops the index entry, so this value cannot be evaluated above it.) The main use case is
 * to extract an element of the index entry that couldn’t otherwise be represented in a field of the record; for
 * example, a {@code CARDINALITY(«array»)} expression materialized in the index.
 *
 * <p>Note that the ordinals are meaningful only with respect to the entry layout of a specific index scan. Any code
 * that creates this value must therefore know that layout, and any rewrite that moves this value must keep it above its
 * associated scan and below any operator that builds a new result.
 *
 * @see IndexEntryObjectValue
 */
@API(API.Status.EXPERIMENTAL)
public class IndexEntryColumnValue extends AbstractValue implements LeafValue {
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Entry-Column-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final TupleSource source;
    @Nonnull
    private final ImmutableIntArray ordinalPath;
    @Nonnull
    private final Type resultType;

    public IndexEntryColumnValue(@Nonnull final CorrelationIdentifier alias,
                                 @Nonnull final TupleSource source,
                                 @Nonnull final ImmutableIntArray ordinalPath,
                                 @Nonnull final Type resultType) {
        Verify.verify(resultType.isPrimitive() || resultType.isEnum() || resultType.isUuid());
        this.alias = alias;
        this.source = source;
        this.ordinalPath = ordinalPath;
        this.resultType = resultType;
    }

    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    public TupleSource getSource() {
        return source;
    }

    @Nonnull
    public ImmutableIntArray getOrdinalPath() {
        return ordinalPath;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of(alias);
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return new IndexEntryColumnValue(targetAlias, source, ordinalPath, resultType);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final var queryResult = (QueryResult)context.getBinding(Bindings.Internal.CORRELATION, alias);
        final IndexEntry indexEntry = Verify.verifyNotNull(queryResult.getIndexEntry());
        return TupleFieldsHelper.tupleValueToRuntimeValue(getForOrdinalPath(indexEntry, source, ordinalPath), resultType);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final var otherColumnValue = (IndexEntryColumnValue)other;
                    return alias.equals(otherColumnValue.alias) &&
                            source == otherColumnValue.source &&
                            ordinalPath.equals(otherColumnValue.ordinalPath);
                });
    }

    @Override
    public int hashCodeWithoutChildren() {
        return planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, ordinalPath, source);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        Verify.verify(Iterables.isEmpty(explainSuppliers));
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addAliasReference(alias).addOptionalWhitespace().addToString(".").addOptionalWhitespace()
                .addKeyword(source.toString()).addToString(":")
                .addIdentifier(ordinalPath.toString()));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public PIndexEntryColumnValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PIndexEntryColumnValue.newBuilder()
                .setAlias(alias.getId())
                .setSource(source.toProto(serializationContext));
        ordinalPath.forEach(builder::addOrdinalPath);
        return builder.setResultType(resultType.toTypeProto(serializationContext)).build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setIndexEntryColumnValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static IndexEntryColumnValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PIndexEntryColumnValue indexEntryColumnValueProto) {
        final ImmutableIntArray.Builder ordinalPathBuilder = ImmutableIntArray.builder();
        for (int i = 0; i < indexEntryColumnValueProto.getOrdinalPathCount(); i ++) {
            ordinalPathBuilder.add(indexEntryColumnValueProto.getOrdinalPath(i));
        }

        return new IndexEntryColumnValue(
                CorrelationIdentifier.of(Objects.requireNonNull(indexEntryColumnValueProto.getAlias())),
                TupleSource.fromProto(serializationContext,
                        Objects.requireNonNull(indexEntryColumnValueProto.getSource())),
                ordinalPathBuilder.build(),
                Type.fromTypeProto(serializationContext,
                        Objects.requireNonNull(indexEntryColumnValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexEntryColumnValue, IndexEntryColumnValue> {
        @Nonnull
        @Override
        public Class<PIndexEntryColumnValue> getProtoMessageClass() {
            return PIndexEntryColumnValue.class;
        }

        @Nonnull
        @Override
        public IndexEntryColumnValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PIndexEntryColumnValue indexEntryColumnValueProto) {
            return IndexEntryColumnValue.fromProto(serializationContext, indexEntryColumnValueProto);
        }
    }
}

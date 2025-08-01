/*
 * IndexEntryObjectValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PIndexEntryObjectValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.Internal;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.getForOrdinalPath;

/**
 * Represents a value that references into an index entry in the bindings of {@link EvaluationContext} by means of an
 * ordinal path (dewey id).
 */
public class IndexEntryObjectValue extends AbstractValue implements LeafValue, Value.RangeMatchableValue {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Entry-Object-Value");

    @Nonnull
    private final CorrelationIdentifier indexEntryAlias;
    @Nonnull
    private final TupleSource source;
    @Nonnull
    private final ImmutableIntArray ordinalPath;

    @Nonnull
    private final Type resultType;

    public IndexEntryObjectValue(@Nonnull final CorrelationIdentifier alias,
                                 @Nonnull final TupleSource source,
                                 @Nonnull final ImmutableIntArray ordinalPath,
                                 @Nonnull final Type resultType) {
        Verify.verify(resultType.isPrimitive() || resultType.isEnum() || resultType.isUuid());
        this.indexEntryAlias = alias;
        this.source = source;
        this.ordinalPath = ordinalPath;
        this.resultType = resultType;
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of();
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> ordinalPath.equals(((IndexEntryObjectValue)other).ordinalPath));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var indexEntry = Objects.requireNonNull((IndexEntry)context.getBinding(Bindings.Internal.CORRELATION, indexEntryAlias));

        final var tuple = (source == TupleSource.KEY ? indexEntry.getKey() : indexEntry.getValue());
        var value = getForOrdinalPath(tuple, ordinalPath);
        if (value == null) {
            return null;
        }

        switch (resultType.getTypeCode()) {
            case INT:
                return ((Long)value).intValue();
            case BYTES:
                return ZeroCopyByteString.wrap((byte[])value);
            case ENUM:
                return (Internal.EnumLite)() -> ((Long)value).intValue();
            default:
                return value;
        }
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
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword(source.toString())
                .addOptionalWhitespace().addToString(":").addIdentifier(ordinalPath.toString()));
    }

    @Nonnull
    @Override
    public PIndexEntryObjectValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PIndexEntryObjectValue.newBuilder()
                .setIndexEntryAlias(indexEntryAlias.getId())
                .setSource(source.toProto(serializationContext));
        ordinalPath.forEach(builder::addOrdinalPath);
        return builder.setResultType(resultType.toTypeProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setIndexEntryObjectValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static String bindingName() {
        return Bindings.Internal.CORRELATION.bindingName(Quantifier.current().getId());
    }

    @Nonnull
    public static IndexEntryObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PIndexEntryObjectValue indexEntryObjectValueProto) {
        final ImmutableIntArray.Builder ordinalPathBuilder = ImmutableIntArray.builder();
        for (int i = 0; i < indexEntryObjectValueProto.getOrdinalPathCount(); i ++) {
            ordinalPathBuilder.add(indexEntryObjectValueProto.getOrdinalPath(i));
        }

        return new IndexEntryObjectValue(
                CorrelationIdentifier.of(Objects.requireNonNull(indexEntryObjectValueProto.getIndexEntryAlias())),
                TupleSource.fromProto(serializationContext,
                        Objects.requireNonNull(indexEntryObjectValueProto.getSource())),
                ordinalPathBuilder.build(),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(indexEntryObjectValueProto.getResultType())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexEntryObjectValue, IndexEntryObjectValue> {
        @Nonnull
        @Override
        public Class<PIndexEntryObjectValue> getProtoMessageClass() {
            return PIndexEntryObjectValue.class;
        }

        @Nonnull
        @Override
        public IndexEntryObjectValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PIndexEntryObjectValue indexEntryObjectValue) {
            return IndexEntryObjectValue.fromProto(serializationContext, indexEntryObjectValue);
        }
    }
}

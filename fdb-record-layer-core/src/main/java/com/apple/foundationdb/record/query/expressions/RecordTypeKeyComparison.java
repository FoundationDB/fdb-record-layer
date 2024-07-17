/*
 * RecordTypeKeyComparison.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PComparison;
import com.apple.foundationdb.record.planprotos.PRecordTypeComparison;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link QueryComponent} that implements checking for a given record type.
 * Not normally used in queries.
 */
@API(API.Status.MAINTAINED)
public class RecordTypeKeyComparison implements ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Type-Key-Comparison");

    @Nonnull
    private final RecordTypeComparison comparison;

    public RecordTypeKeyComparison(@Nonnull String recordTypeName) {
        this.comparison = new RecordTypeComparison(recordTypeName);
    }

    public static boolean hasRecordTypeKeyComparison(@Nonnull ScanComparisons scanComparisons) {
        return scanComparisons.getEqualitySize() > 0 && scanComparisons.getEqualityComparisons().get(0) instanceof RecordTypeComparison;
    }

    public static Set<String> recordTypeKeyComparisonTypes(@Nonnull ScanComparisons scanComparisons) {
        return Collections.singleton(((RecordTypeComparison)scanComparisons.getEqualityComparisons().get(0)).recordTypeName);
    }

    @Override
    public String getName() {
        return getComparison().typelessString();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return getComparison().eval(store, context, message);
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        // Usable against any record type.
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nonnull final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {
        return GraphExpansion.ofPredicate(
                new RecordTypeValue(QuantifiedObjectValue.of(baseQuantifier))
                        .withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS,
                                Objects.requireNonNull(comparison.getComparand()))));
    }

    @Override
    public String toString() {
        return getComparison().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RecordTypeKeyComparison that = (RecordTypeKeyComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getComparison().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getComparison());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, getComparison());
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        throw new UnsupportedOperationException("Cannot change comparison");
    }

    /**
     * Equality comparison to check for records of a particular record type.
     */
    public static class RecordTypeComparison implements Comparisons.Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Type-Comparison");

        @Nonnull
        private final String recordTypeName;

        RecordTypeComparison(@Nonnull String recordTypeName) {
            this.recordTypeName = recordTypeName;
        }

        @Nullable
        @Override
        public Boolean eval(@Nonnull FDBRecordStoreBase<?> store, @Nonnull EvaluationContext context, @Nullable Object value) {
            if (value == null) {
                return null;
            }
            return ((Message)value).getDescriptorForType().getName().equals(recordTypeName);
        }

        @Override
        public void validate(@Nonnull Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // Do not actually apply to any particular field.
        }

        @Nonnull
        @Override
        public Comparisons.Comparison translateCorrelations(@Nonnull final TranslationMap translationMap) {
            return this;
        }

        @Nonnull
        @Override
        public Comparisons.Type getType() {
            return Comparisons.Type.EQUALS;
        }

        @Nonnull
        @Override
        public Comparisons.Comparison withType(@Nonnull final Comparisons.Type newType) {
            if (newType == Comparisons.Type.EQUALS) {
                return this;
            }
            throw new RecordCoreException("'" + RecordTypeKeyComparison.class.getSimpleName() + "' expects '" + Comparisons.Type.EQUALS.name() + "' comparison only");
        }

        @Nullable
        @Override
        public Object getComparand(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
            if (store == null) {
                throw Comparisons.EvaluationContextRequiredException.instance();
            }
            return store.getRecordMetaData().getIndexableRecordType(recordTypeName).getRecordTypeKey();
        }

        @Nonnull
        @Override
        public String typelessString() {
            return recordTypeName;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return PlanHashable.objectPlanHash(mode, recordTypeName);
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, recordTypeName);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return HashUtils.queryHash(hashKind, BASE_HASH, recordTypeName);
        }

        @Override
        public String toString() {
            return "IS " + recordTypeName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RecordTypeComparison that = (RecordTypeComparison)o;
            return Objects.equals(recordTypeName, that.recordTypeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordTypeName);
        }

        @Nonnull
        @Override
        public PRecordTypeComparison toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PRecordTypeComparison.newBuilder().setRecordTypeName(recordTypeName).build();
        }

        @Nonnull
        @Override
        public PComparison toComparisonProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setRecordTypeComparison(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static RecordTypeComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PRecordTypeComparison recordTypeComparisonProto) {
            return new RecordTypeComparison(Objects.requireNonNull(recordTypeComparisonProto.getRecordTypeName()));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PRecordTypeComparison, RecordTypeComparison> {
            @Nonnull
            @Override
            public Class<PRecordTypeComparison> getProtoMessageClass() {
                return PRecordTypeComparison.class;
            }

            @Nonnull
            @Override
            public RecordTypeComparison fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PRecordTypeComparison recordTypeComparisonProto) {
                return RecordTypeComparison.fromProto(serializationContext, recordTypeComparisonProto);
            }
        }
    }
}

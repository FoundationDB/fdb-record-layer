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
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link QueryComponent} that implements checking for a given record type.
 * Not normally used in queries.
 */
@API(API.Status.UNSTABLE)
public class RecordTypeKeyComparison implements ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Type-Key-Comparison");

    private final RecordTypeComparison comparison;

    public RecordTypeKeyComparison(String recordTypeName) {
        this.comparison = new RecordTypeComparison(recordTypeName);
    }

    public static boolean hasRecordTypeKeyComparison(ScanComparisons scanComparisons) {
        return scanComparisons.getEqualitySize() > 0 && scanComparisons.getEqualityComparisons().get(0) instanceof RecordTypeComparison;
    }

    public static Set<String> recordTypeKeyComparisonTypes(ScanComparisons scanComparisons) {
        return Collections.singleton(((RecordTypeComparison)scanComparisons.getEqualityComparisons().get(0)).recordTypeName);
    }

    @Override
    public String getName() {
        return getComparison().typelessString();
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(FDBRecordStoreBase<M> store, EvaluationContext context,
                                                   @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return getComparison().eval(store, context, message);
    }

    @Override
    public void validate(Descriptors.Descriptor descriptor) {
        // Usable against any record type.
    }

    @Override
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public GraphExpansion expand(final Quantifier.ForEach baseQuantifier,
                                 final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 final List<String> fieldNamePrefix) {
        // Note: this is broken. The comparison requires access to the store's meta-data in order
        // to look up the record type key in order to produce an appropriate comparison.
        // This shouldn't be too much of a problem, as this component shouldn't appear in
        // the kinds of queries that the Cascades planner can plan, but alas.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/3813
        return GraphExpansion.ofPredicate(
                new RecordTypeValue(QuantifiedRecordValue.of(baseQuantifier))
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
        RecordTypeKeyComparison that = (RecordTypeKeyComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash(final PlanHashMode mode) {
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
    public QueryComponent withOtherComparison(Comparisons.Comparison newComparison) {
        if (newComparison instanceof RecordTypeComparison) {
            final String newRecordTypeName = ((RecordTypeComparison)newComparison).getRecordTypeName();
            if (comparison.getRecordTypeName().equals(newRecordTypeName)) {
                return this;
            }
            return new RecordTypeKeyComparison(newRecordTypeName);
        }
        throw new UnsupportedOperationException("Cannot change comparison");
    }

    /**
     * Equality comparison to check for records of a particular record type.
     */
    public static class RecordTypeComparison implements Comparisons.Comparison {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Type-Comparison");

        private final String recordTypeName;

        RecordTypeComparison(String recordTypeName) {
            this.recordTypeName = recordTypeName;
        }

        @Nullable
        @Override
        public Boolean eval(@Nullable FDBRecordStoreBase<?> store, EvaluationContext context, @Nullable Object value) {
            if (value == null) {
                return null;
            }
            return ((Message)value).getDescriptorForType().getName().equals(recordTypeName);
        }

        @Override
        public void validate(Descriptors.FieldDescriptor descriptor, boolean fannedOut) {
            // Do not actually apply to any particular field.
        }

        @Override
        public Optional<Comparisons.Comparison> replaceValuesMaybe(final Function<Value, Optional<Value>> replacementFunction) {
            return Optional.of(this);
        }

        @Override
        public Comparisons.Comparison translateCorrelations(final TranslationMap translationMap,
                                                            final boolean shouldSimplifyValues) {
            return this;
        }

        public String getRecordTypeName() {
            return recordTypeName;
        }

        @Override
        public Comparisons.Type getType() {
            return Comparisons.Type.EQUALS;
        }

        @Override
        public Comparisons.Comparison withType(final Comparisons.Type newType) {
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

        @Override
        public String typelessString() {
            return recordTypeName;
        }

        @Override
        public int planHash(final PlanHashMode mode) {
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
        public String toString() {
            return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public ExplainTokensWithPrecedence explain() {
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addKeyword("IS")
                    .addWhitespace().addIdentifier(ProtoUtils.toUserIdentifier(typelessString())));
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

        @Override
        public PRecordTypeComparison toProto(final PlanSerializationContext serializationContext) {
            return PRecordTypeComparison.newBuilder().setRecordTypeName(recordTypeName).build();
        }

        @Override
        public PComparison toComparisonProto(final PlanSerializationContext serializationContext) {
            return PComparison.newBuilder().setRecordTypeComparison(toProto(serializationContext)).build();
        }

        @SuppressWarnings("unused")
        public static RecordTypeComparison fromProto(final PlanSerializationContext serializationContext,
                                                     final PRecordTypeComparison recordTypeComparisonProto) {
            return new RecordTypeComparison(Objects.requireNonNull(recordTypeComparisonProto.getRecordTypeName()));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PRecordTypeComparison, RecordTypeComparison> {
            @Override
            public Class<PRecordTypeComparison> getProtoMessageClass() {
                return PRecordTypeComparison.class;
            }

            @Override
            public RecordTypeComparison fromProto(final PlanSerializationContext serializationContext,
                                                  final PRecordTypeComparison recordTypeComparisonProto) {
                return RecordTypeComparison.fromProto(serializationContext, recordTypeComparisonProto);
            }
        }
    }
}

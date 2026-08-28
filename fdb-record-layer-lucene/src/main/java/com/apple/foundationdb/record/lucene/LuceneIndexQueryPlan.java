/*
 * LuceneIndexQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.planprotos.PLuceneIndexQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.PlanWithOrderingKey;
import com.apple.foundationdb.record.query.plan.PlanWithStoredFields;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithExplain;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Lucene query plan for including search-related scan parameters.
 */
@SuppressWarnings("PMD.OverrideBothEqualsAndHashcode")
@SpotBugsSuppressWarnings("HE_EQUALS_NO_HASHCODE")
public class LuceneIndexQueryPlan extends RecordQueryIndexPlan implements PlanWithOrderingKey, PlanWithStoredFields, RecordQueryPlanWithExplain {
    @Nullable
    private final PlanOrderingKey planOrderingKey;
    @Nullable
    private final List<KeyExpression> storedFields;

    protected LuceneIndexQueryPlan(final PlanSerializationContext serializationContext,
                                   final PLuceneIndexQueryPlan luceneIndexQueryPlanProto) {
        super(serializationContext, Objects.requireNonNull(luceneIndexQueryPlanProto.getSuper()));
        this.planOrderingKey = null; // TODO
        Verify.verify(luceneIndexQueryPlanProto.hasHasStoredFields());
        if (luceneIndexQueryPlanProto.getHasStoredFields()) {
            this.storedFields = Lists.newArrayList();
            for (int i = 0; i < luceneIndexQueryPlanProto.getStoredFieldsCount(); i ++) {
                this.storedFields.add(KeyExpression.fromProto(luceneIndexQueryPlanProto.getStoredFields(i)));
            }
        } else {
            this.storedFields = null;
        }
    }

    protected LuceneIndexQueryPlan(String indexName, LuceneScanParameters scanParameters,
                                   FetchIndexRecords fetchIndexRecords, boolean reverse,
                                   @Nullable PlanOrderingKey planOrderingKey, @Nullable List<KeyExpression> storedFields) {
        super(indexName, null, scanParameters, IndexFetchMethod.SCAN_AND_FETCH, fetchIndexRecords, reverse, false);
        this.planOrderingKey = planOrderingKey;
        this.storedFields = storedFields;
    }

    /**
     * Auto-Complete and Spell-Check scan has their own implementation for {@link IndexKeyValueToPartialRecord} to build partial records,
     * so they are not appropriate for the optimization by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan}.
     */
    @Override
    public boolean allowedForCoveringIndexPlan() {
        return true;
    }

    @Override
    public boolean hasComparisons() {
        return true;
    }

    @Override
    public Set<Comparisons.Comparison> getComparisons() {
        final var resultBuilder = ImmutableSet.<Comparisons.Comparison>builder();
        if (scanParameters instanceof LuceneScanParameters) {
            final var luceneScanParameters = (LuceneScanParameters)scanParameters;
            final var groupScanComparisons = luceneScanParameters.getGroupComparisons();
            resultBuilder.addAll(groupScanComparisons.getEqualityComparisons());
            resultBuilder.addAll(groupScanComparisons.getInequalityComparisons());

            if (luceneScanParameters instanceof LuceneScanQueryParameters) {
                final var luceneScanQueryParameters = (LuceneScanQueryParameters)luceneScanParameters;
                final var queryClause = luceneScanQueryParameters.getQuery();
                resultBuilder.addAll(collectComparisons(queryClause));
            }
        }
        return resultBuilder.build();
    }

    private Set<Comparisons.Comparison> collectComparisons(final LuceneQueryClause queryClause) {
        final var resultBuilder = ImmutableSet.<Comparisons.Comparison>builder();

        if (queryClause instanceof LuceneQueryFieldComparisonClause) {
            resultBuilder.add(((LuceneQueryFieldComparisonClause)queryClause).getComparison());
        } else if (queryClause instanceof LuceneBooleanQuery) {
            for (final var andTerm : ((LuceneBooleanQuery)queryClause).getChildren()) {
                resultBuilder.addAll(collectComparisons(andTerm));
            }
        }
        return resultBuilder.build();
    }

    @Nullable
    @Override
    public PlanOrderingKey getPlanOrderingKey() {
        return planOrderingKey;
    }

    @Nullable
    public List<KeyExpression> getStoredFields() {
        return storedFields;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public void getStoredFields(List<KeyExpression> keyFields, List<KeyExpression> nonStoredFields,
                                List<KeyExpression> otherFields) {
        int i = 0;
        while (i < nonStoredFields.size()) {
            KeyExpression origField = nonStoredFields.get(i);
            KeyExpression field = removeLuceneAnnotations(origField);
            if (field != origField) {
                nonStoredFields.set(i, field);
                int j = keyFields.indexOf(origField);
                if (j >= 0) {
                    keyFields.set(j, field);
                }
            }
            if (storedFields != null && storedFields.contains(origField)) {
                nonStoredFields.remove(i);
            } else {
                i++;
            }
        }
        if (planOrderingKey != null) {
            // These are available by extending the IndexEntry.
            for (KeyExpression orderingKey : planOrderingKey.getKeys()) {
                if (orderingKey instanceof LuceneFunctionKeyExpression.LuceneSortBy) {
                    otherFields.add(orderingKey);
                }
            }
        }
    }

    // Unwrap functions that are just annotations.
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static KeyExpression removeLuceneAnnotations(KeyExpression field) {
        if (field instanceof NestingKeyExpression) {
            KeyExpression origChild = ((NestingKeyExpression)field).getChild();
            KeyExpression child = removeLuceneAnnotations(origChild);
            if (child == origChild) {
                return field;
            }
            return new NestingKeyExpression(((NestingKeyExpression)field).getParent(), child);
        }
        while (field instanceof LuceneFunctionKeyExpression) {
            if (field instanceof LuceneFunctionKeyExpression.LuceneSorted) {
                field = ((LuceneFunctionKeyExpression.LuceneSorted)field).getSortedExpression();
            } else if (field instanceof LuceneFunctionKeyExpression.LuceneStored) {
                field = ((LuceneFunctionKeyExpression.LuceneStored)field).getStoredExpression();
            } else {
                break;
            }
        }
        return field;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final LuceneIndexQueryPlan that = (LuceneIndexQueryPlan)o;
        return Objects.equals(planOrderingKey, that.planOrderingKey) &&
               Objects.equals(storedFields, that.storedFields);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(super.computeHashCodeWithoutChildren(), planOrderingKey, storedFields);
    }

    @Override
    protected RecordQueryIndexPlan withIndexScanParameters(final IndexScanParameters newIndexScanParameters) {
        Verify.verify(newIndexScanParameters instanceof LuceneScanParameters);
        Verify.verify(newIndexScanParameters.getScanType().equals(LuceneScanTypes.BY_LUCENE));
        return new LuceneIndexQueryPlan(getIndexName(), (LuceneScanParameters)newIndexScanParameters, getFetchIndexRecords(), reverse, planOrderingKey, storedFields);
    }

    public static LuceneIndexQueryPlan of(final String indexName, final LuceneScanParameters scanParameters,
                                          final FetchIndexRecords fetchIndexRecords, final boolean reverse,
                                          @Nullable final PlanOrderingKey planOrderingKey, @Nullable final List<KeyExpression> storedFields) {
        if (scanParameters.getScanType().equals(LuceneScanTypes.BY_LUCENE)) {
            return new LuceneIndexQueryPlan(indexName, scanParameters, fetchIndexRecords, reverse, planOrderingKey, storedFields);
        } else if (scanParameters.getScanType().equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK)) {
            return new LuceneIndexSpellCheckQueryPlan(indexName, scanParameters, fetchIndexRecords, reverse, planOrderingKey, storedFields);
        }
        throw new RecordCoreException("unknown lucene scan warranted by caller");
    }

    @Override
    public ExplainTokensWithPrecedence explain() {
        return ExplainTokensWithPrecedence.of(
                new ExplainTokens().addKeyword("LISCAN").addOptionalWhitespace().addOpeningParen()
                        .addNested(ExplainPlanVisitor.indexDetails(this))
                        .addOptionalWhitespace().addClosingParen());
    }

    @Override
    public Message toProto(final PlanSerializationContext serializationContext) {
        return toLuceneIndexPlanProto(serializationContext);
    }

    public PLuceneIndexQueryPlan toLuceneIndexPlanProto(final PlanSerializationContext serializationContext) {
        final PLuceneIndexQueryPlan.Builder builder = PLuceneIndexQueryPlan.newBuilder()
                .setSuper(toRecordQueryIndexPlanProto(serializationContext));
        builder.setHasStoredFields(storedFields != null);
        if (storedFields != null) {
            for (final KeyExpression storedField : storedFields) {
                builder.addStoredFields(storedField.toKeyExpression());
            }
        }
        // TODO plan ordering key
        return builder.build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder()
                .setAdditionalPlans(PlanSerialization.protoObjectToAny(serializationContext,
                        toLuceneIndexPlanProto(serializationContext)))
                .build();
    }

    @SuppressWarnings("unused")
    public static LuceneIndexQueryPlan fromProto(final PlanSerializationContext serializationContext,
                                                 final PLuceneIndexQueryPlan luceneIndexQueryPlanProto) {
        return new LuceneIndexQueryPlan(serializationContext, luceneIndexQueryPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PLuceneIndexQueryPlan, LuceneIndexQueryPlan> {
        @Override
        public Class<PLuceneIndexQueryPlan> getProtoMessageClass() {
            return PLuceneIndexQueryPlan.class;
        }

        @Override
        public LuceneIndexQueryPlan fromProto(final PlanSerializationContext serializationContext,
                                              final PLuceneIndexQueryPlan luceneIndexQueryPlanProto) {
            return LuceneIndexQueryPlan.fromProto(serializationContext, luceneIndexQueryPlanProto);
        }
    }
}

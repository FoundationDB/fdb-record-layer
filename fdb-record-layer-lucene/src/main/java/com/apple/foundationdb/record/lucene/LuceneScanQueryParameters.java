/*
 * LuceneScanQueryParameters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.planprotos.PLuceneScanQueryParameters;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Scan parameters for making a {@link LuceneScanQuery}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanQueryParameters extends LuceneScanParameters implements PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Scan-Query");
    public static final Logger LOGGER = LoggerFactory.getLogger(LuceneScanQueryParameters.class);

    @Nonnull
    final LuceneQueryClause query;
    @Nullable
    final Sort sort;
    @Nullable
    final List<String> storedFields;
    @Nullable
    final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes;

    @Nullable
    final LuceneQueryHighlightParameters luceneQueryHighlightParameters;

    @SpotBugsSuppressWarnings("NP_STORE_INTO_NONNULL_FIELD") // TODO remove this once we have a proper implementation
    protected LuceneScanQueryParameters(@Nonnull final PlanSerializationContext serializationContext,
                                        @Nonnull final PLuceneScanQueryParameters luceneScanQueryParametersProto) {
        super(serializationContext, Objects.requireNonNull(luceneScanQueryParametersProto.getSuper()));
        // TODO replace stub by extracting info out of the proto
        //noinspection DataFlowIssue
        this.query = null;
        this.sort = null;
        this.storedFields = null;
        this.storedFieldTypes = null;
        this.luceneQueryHighlightParameters = null;
    }

    public LuceneScanQueryParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull LuceneQueryClause query) {
        this(groupComparisons, query, null, null, null, null);
    }

    public LuceneScanQueryParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull LuceneQueryClause query,
                                     @Nullable Sort sort,
                                     @Nullable List<String> storedFields, @Nullable List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes,
                                     @Nullable LuceneQueryHighlightParameters luceneQueryHighlightParameters) {
        super(LuceneScanTypes.BY_LUCENE, groupComparisons);
        this.query = query;
        this.sort = sort;
        this.storedFields = storedFields;
        this.storedFieldTypes = storedFieldTypes;
        this.luceneQueryHighlightParameters = luceneQueryHighlightParameters;
    }

    @Nonnull
    public LuceneQueryClause getQuery() {
        return query;
    }

    @Nullable
    public Sort getSort() {
        return sort;
    }

    @Nullable
    public List<String> getStoredFields() {
        return storedFields;
    }

    @Nullable
    public List<LuceneIndexExpressions.DocumentFieldType> getStoredFieldTypes() {
        return storedFieldTypes;
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(
                mode,
                BASE_HASH,
                scanType,
                groupComparisons,
                query,
                sort == null ? null : sort.toString(), // `Sort.hashCode` ends up hashing an enum so the hash would not be stable
                storedFields,
                storedFieldTypes);
    }

    @Nonnull
    @Override
    public LuceneScanQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.build("LuceneScanQueryParameters binding")
                    .addKeyAndValue(LogMessageKeys.INDEX_NAME, index.getName())
                    .addKeyAndValue(LogMessageKeys.QUERY, this.query)
                    .toString());
        }

        final LuceneQueryClause.BoundQuery boundQuery = query.bind(store, index, context);
        if (luceneQueryHighlightParameters != null) {
            luceneQueryHighlightParameters.query = boundQuery.getLuceneQuery();
            Objects.requireNonNull(boundQuery.getHighlightingTermsMap());
        }
        return new LuceneScanQuery(scanType, getGroupKey(store, context), boundQuery.getLuceneQuery(),
                sort, storedFields, storedFieldTypes, luceneQueryHighlightParameters, boundQuery.getHighlightingTermsMap());
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        return getGroupScanDetails() + " " + query;
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        super.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        query.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        if (sort != null) {
            detailsBuilder.add("sort: {{sort}}");
            attributeMapBuilder.put("sort", Attribute.gml(sort.toString()));
        }
        if (storedFields != null) {
            StringBuilder stored = new StringBuilder();
            for (int i = 0; i < storedFields.size(); i++) {
                if (i > 0) {
                    stored.append(", ");
                }
                stored.append(storedFields.get(i) + ":" + storedFieldTypes.get(i));
            }
            detailsBuilder.add("stored: {{stored}}");
            attributeMapBuilder.put("stored", Attribute.gml(stored.toString()));
        }
    }

    @Nonnull
    @Override
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap) {
        return this;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (!super.equals(other)) {
            return false;
        }

        final LuceneScanQueryParameters that = (LuceneScanQueryParameters)other;

        return query.equals(that.query) && Objects.equals(sort, that.sort);
    }

    @Override
    public int semanticHashCode() {
        int result = super.hashCode();
        result = 31 * result + query.hashCode();
        if (sort != null) {
            result = 31 * result + sort.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return super.toString() + " " + query;
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
    public PLuceneScanQueryParameters toProto(@Nonnull final PlanSerializationContext serializationContext) {
        // TODO replace stub
        return PLuceneScanQueryParameters.newBuilder()
                .setSuper(toLuceneScanParametersProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder()
                .setAdditionalIndexScanParameters(PlanSerialization.protoObjectToAny(serializationContext, toProto(serializationContext)))
                .build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static LuceneScanQueryParameters fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PLuceneScanQueryParameters luceneScanQueryParametersProto) {
        return new LuceneScanQueryParameters(serializationContext, luceneScanQueryParametersProto);
    }

    /**
     * The parameters for highlighting matching terms of a Lucene search.
     */
    public static class LuceneQueryHighlightParameters {
        private final int snippedSize;

        //the maximum number of times the query will be matched during summarization
        private final int maxMatchCount;
        private Query query;

        /**
         * Create parameter for lucene query highlights.
         *
         * @param snippetSize The size of the snippets that should be returned by the query. If zero or less, return the
         * entire text
         * @param maxMatchCount The maximum number of times a query will match during highlighting before the highlighter
         * stops
         */
        public LuceneQueryHighlightParameters(int snippetSize, int maxMatchCount) {
            this(snippetSize, maxMatchCount, null);
        }

        public LuceneQueryHighlightParameters(int snippetSize, int maxMatchCount, Query theQuery) {
            this.snippedSize = snippetSize;
            this.maxMatchCount = maxMatchCount;
            this.query = theQuery;
        }

        public Query getQuery() {
            return query;
        }

        public boolean isCutSnippets() {
            return snippedSize > 1;
        }

        public int getSnippedSize() {
            return snippedSize;
        }

        public int getMaxMatchCount() {
            return maxMatchCount;
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PLuceneScanQueryParameters, LuceneScanQueryParameters> {
        @Nonnull
        @Override
        public Class<PLuceneScanQueryParameters> getProtoMessageClass() {
            return PLuceneScanQueryParameters.class;
        }

        @Nonnull
        @Override
        public LuceneScanQueryParameters fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PLuceneScanQueryParameters luceneScanQueryParametersProto) {
            return LuceneScanQueryParameters.fromProto(serializationContext, luceneScanQueryParametersProto);
        }
    }
}

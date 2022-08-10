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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.Sort;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Scan parameters for making a {@link LuceneScanQuery}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanQueryParameters extends LuceneScanParameters {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Scan-Query");

    @Nonnull
    final LuceneQueryClause query;
    @Nullable
    final Sort sort;
    @Nullable
    final List<String> storedFields;
    @Nullable
    final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes;

    public LuceneScanQueryParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull LuceneQueryClause query) {
        this(groupComparisons, query, null, null, null);
    }

    public LuceneScanQueryParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull LuceneQueryClause query,
                                     @Nullable Sort sort,
                                     @Nullable List<String> storedFields, @Nullable List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes) {
        super(LuceneScanTypes.BY_LUCENE, groupComparisons);
        this.query = query;
        this.sort = sort;
        this.storedFields = storedFields;
        this.storedFieldTypes = storedFieldTypes;
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
    public int planHash(@Nonnull PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, scanType, groupComparisons, query, sort, storedFields, storedFieldTypes);
    }

    @Nonnull
    @Override
    public LuceneScanQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        return new LuceneScanQuery(scanType, getGroupKey(store, context), query.bind(store, index, context),
                sort, storedFields, storedFieldTypes);
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
}

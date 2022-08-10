/*
 * LuceneScanAutoCompleteParameters.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * Scan parameters for making a {@link LuceneScanAutoComplete}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanAutoCompleteParameters extends LuceneScanParameters {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Scan-Auto-Complete");

    @Nonnull
    final String key;
    final boolean isParameter;

    final boolean highlight;

    protected LuceneScanAutoCompleteParameters(@Nonnull ScanComparisons groupComparisons, @Nonnull String key, boolean isParameter, boolean highlight) {
        super(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE, groupComparisons);
        this.key = key;
        this.isParameter = isParameter;
        this.highlight = highlight;
    }

    @Override
    public int planHash(@Nonnull PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, scanType, groupComparisons, key, isParameter);
    }

    @Nonnull
    @Override
    public LuceneScanAutoComplete bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        String keyToComplete = isParameter ? (String)context.getBinding(key) : key;
        return new LuceneScanAutoComplete(scanType, getGroupKey(store, context), keyToComplete, highlight);
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        return getGroupScanDetails() + " " + (isParameter ? "$" : "") + key;
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        super.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        if (isParameter) {
            detailsBuilder.add("param: {{param}}");
            attributeMapBuilder.put("param", Attribute.gml(key));
        } else {
            detailsBuilder.add("key: {{key}}");
            attributeMapBuilder.put("key", Attribute.gml(key));
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
        return this;
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

        final LuceneScanAutoCompleteParameters that = (LuceneScanAutoCompleteParameters)other;

        if (isParameter != that.isParameter) {
            return false;
        }
        return key.equals(that.key);
    }

    @Override
    public int semanticHashCode() {
        int result = super.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + (isParameter ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return super.toString() + " " + (isParameter ? "$" : "") + key;
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

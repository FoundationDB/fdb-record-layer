/*
 * LuceneScanSpellCheckParameters.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.planprotos.PLuceneScanSpellCheckParameters;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.util.LogMessageKeys;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.apple.foundationdb.record.planprotos.LuceneRecordQueryPlanProto.luceneScanSpellCheckParameters;

/**
 * Scan parameters for making a {@link LuceneScanSpellCheck}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanSpellCheckParameters extends LuceneScanParameters implements PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Scan-Spell-Check");

    @Nonnull
    final String key;
    final boolean isParameter;

    protected LuceneScanSpellCheckParameters(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PLuceneScanSpellCheckParameters luceneScanSpellCheckParametersProto) {
        super(serializationContext, Objects.requireNonNull(luceneScanSpellCheckParametersProto.getSuper()));
        // TODO replace stub by extracting info out of the proto
        this.key = "TODO";
        this.isParameter = false;
    }

    protected LuceneScanSpellCheckParameters(@Nonnull ScanComparisons groupComparisons,
                                             @Nonnull String key, boolean isParameter) {
        super(LuceneScanTypes.BY_LUCENE_SPELL_CHECK, groupComparisons);
        this.key = key;
        this.isParameter = isParameter;
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, scanType, groupComparisons, key, isParameter);
    }

    @Nonnull
    @Override
    public LuceneScanSpellCheck bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        List<String> fields = indexTextFields(index, store.getRecordMetaData());
        String wordToSpellCheck = isParameter ? (String)context.getBinding(key) : key;
        // TODO: Probably want more obvious syntax for this.
        if (wordToSpellCheck.contains(":")) {
            String[] fieldAndWord = wordToSpellCheck.split(":", 2);
            if (fields.stream().noneMatch(name -> name.equals(fieldAndWord[0]))) {
                throw new RecordCoreException("Invalid field name in Lucene index query")
                        .addLogInfo(LogMessageKeys.FIELD_NAME, fieldAndWord[0])
                        .addLogInfo(LogMessageKeys.INDEX_FIELDS, fields);
            }
            fields = List.of(fieldAndWord[0]);
            wordToSpellCheck = fieldAndWord[1];
        }
        return new LuceneScanSpellCheck(scanType, getGroupKey(store, context), fields, wordToSpellCheck);
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

        final LuceneScanSpellCheckParameters that = (LuceneScanSpellCheckParameters)other;

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

    @Nonnull
    @Override
    public PLuceneScanSpellCheckParameters toProto(@Nonnull final PlanSerializationContext serializationContext) {
        // TODO replace stub
        return PLuceneScanSpellCheckParameters.newBuilder()
                .setSuper(toLuceneScanParametersProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder()
                .setExtension(luceneScanSpellCheckParameters, toProto(serializationContext))
                .build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static LuceneScanSpellCheckParameters fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                           @Nonnull final PLuceneScanSpellCheckParameters luceneScanSpellCheckParametersProto) {
        return new LuceneScanSpellCheckParameters(serializationContext, luceneScanSpellCheckParametersProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PLuceneScanSpellCheckParameters, LuceneScanSpellCheckParameters> {
        @Nonnull
        @Override
        public Class<PLuceneScanSpellCheckParameters> getProtoMessageClass() {
            return PLuceneScanSpellCheckParameters.class;
        }

        @Nonnull
        @Override
        public LuceneScanSpellCheckParameters fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PLuceneScanSpellCheckParameters luceneScanSpellCheckParametersProto) {
            return LuceneScanSpellCheckParameters.fromProto(serializationContext, luceneScanSpellCheckParametersProto);
        }
    }
}

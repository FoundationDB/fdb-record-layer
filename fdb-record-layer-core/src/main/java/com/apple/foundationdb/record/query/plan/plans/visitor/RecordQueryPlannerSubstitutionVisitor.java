/*
 * RecordQueryPlannerSubstitutionVisitor.java
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

package com.apple.foundationdb.record.query.plan.plans.visitor;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Visitor interface for performing substitution-type rules on {@link RecordQueryPlan}s.
 */
public abstract class RecordQueryPlannerSubstitutionVisitor {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(RecordQueryPlanner.class);

    @Nonnull
    protected final RecordMetaData recordMetadata;
    @Nullable
    private final KeyExpression commonPrimaryKey;

    public RecordQueryPlannerSubstitutionVisitor(@Nonnull RecordMetaData recordMetadata, @Nullable KeyExpression commonPrimaryKey) {
        this.recordMetadata = recordMetadata;
        this.commonPrimaryKey = commonPrimaryKey;
    }

    public static RecordQueryPlan applyVisitors(@Nonnull RecordQueryPlan plan, @Nonnull RecordMetaData recordMetaData, @Nullable KeyExpression commonPrimaryKey) {
        return plan.accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData, commonPrimaryKey))
                .accept(new UnionVisitor(recordMetaData, commonPrimaryKey))
                .accept(new IntersectionVisitor(recordMetaData, commonPrimaryKey))
                .accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData, commonPrimaryKey));
    }

    @Nonnull
    public abstract RecordQueryPlan visit(@Nonnull RecordQueryPlan recordQueryPlan);

    @Nullable
    protected RecordQueryPlan removeIndexFetch(@Nonnull RecordQueryPlan plan) {
        if (plan instanceof RecordQueryPlanWithIndex) {
            RecordQueryPlanWithIndex indexPlan = (RecordQueryPlanWithIndex) plan;
            final IndexKeyValueToPartialRecord keyValueToPartialRecord = generateIndexKeyValueToPartialRecord(indexPlan);
            @Nullable RecordType recordType = getSingleRecordTypeForIndex(indexPlan.getIndexName());
            if (keyValueToPartialRecord != null && recordType != null) {
                return new RecordQueryCoveringIndexPlan(indexPlan, recordType.getName(), keyValueToPartialRecord);
            } else {
                return null;
            }
        } else if (plan instanceof RecordQueryFetchFromPartialRecordPlan) {
            return ((RecordQueryFetchFromPartialRecordPlan)plan).getChild();
        } else {
            return null;
        }
    }

    @Nullable
    private IndexKeyValueToPartialRecord generateIndexKeyValueToPartialRecord(@Nonnull RecordQueryPlanWithIndex indexPlan) {
        Index index = recordMetadata.getIndex(indexPlan.getIndexName());
        final RecordType recordType = getSingleRecordTypeForIndex(indexPlan.getIndexName());
        if (recordType == null) {
            return null;
        }

        final KeyExpression rootExpression = index.getRootExpression();
        final List<KeyExpression> keyFields = KeyExpression.getKeyFields(rootExpression);
        final List<KeyExpression> valueFields = KeyExpression.getValueFields(rootExpression);

        // Like FDBRecordStoreBase.indexEntryKey(), but with key expressions instead of actual values.
        final List<KeyExpression> primaryKeys = commonPrimaryKey == null
                                                ? Collections.emptyList()
                                                : new ArrayList<>(commonPrimaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(primaryKeys);
        keyFields.addAll(primaryKeys);

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType.getDescriptor());

        if (commonPrimaryKey != null) {
            for (KeyExpression primaryKeyField : keyFields) {
                try {
                    RecordQueryPlanner.addCoveringField(primaryKeyField, builder, keyFields, valueFields);
                } catch (Exception e) {
                    logger.warn("duplicate field -> " + primaryKeyField);
                    return null;
                }
            }
        }
        if (!builder.isValid()) {
            return null;
        }
        return builder.build();
    }

    @Nullable
    private RecordType getSingleRecordTypeForIndex(@Nonnull String indexName) {
        Index index = recordMetadata.getIndex(indexName);
        final Collection<RecordType> recordTypes = recordMetadata.recordTypesForIndex(index);

        if (recordTypes.size() != 1) {
            return null;
        } else {
            return Iterables.getOnlyElement(recordTypes);
        }
    }
}

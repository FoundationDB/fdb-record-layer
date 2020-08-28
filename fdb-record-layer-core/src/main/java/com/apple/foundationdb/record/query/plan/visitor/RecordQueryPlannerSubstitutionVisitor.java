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

package com.apple.foundationdb.record.query.plan.visitor;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Visitor interface for performing substitution-type rules on {@link RecordQueryPlan}s.
 */
public abstract class RecordQueryPlannerSubstitutionVisitor {
    @Nonnull
    protected final RecordMetaData recordMetadata;
    @Nonnull
    private final PlannableIndexTypes indexTypes;
    @Nullable
    private final KeyExpression commonPrimaryKey;

    public RecordQueryPlannerSubstitutionVisitor(@Nonnull RecordMetaData recordMetadata,
                                                 @Nonnull PlannableIndexTypes indexTypes,
                                                 @Nullable KeyExpression commonPrimaryKey) {
        this.recordMetadata = recordMetadata;
        this.indexTypes = indexTypes;
        this.commonPrimaryKey = commonPrimaryKey;
    }

    public static RecordQueryPlan applyVisitors(@Nonnull RecordQueryPlan plan, @Nonnull RecordMetaData recordMetaData, @Nonnull PlannableIndexTypes indexTypes, @Nullable KeyExpression commonPrimaryKey) {
        return plan.accept(new FilterVisitor(recordMetaData, indexTypes, commonPrimaryKey))
                .accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData, indexTypes, commonPrimaryKey))
                .accept(new UnionVisitor(recordMetaData, indexTypes, commonPrimaryKey))
                .accept(new IntersectionVisitor(recordMetaData, indexTypes, commonPrimaryKey))
                .accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData, indexTypes, commonPrimaryKey));
    }

    @Nonnull
    public abstract RecordQueryPlan postVisit(@Nonnull RecordQueryPlan recordQueryPlan);

    @Nullable
    public RecordQueryPlan removeIndexFetch(@Nonnull RecordQueryPlan plan, @Nonnull Set<KeyExpression> requiredFields) {
        return removeIndexFetch(recordMetadata, indexTypes, commonPrimaryKey, plan, requiredFields);
    }

    @Nullable
    public static RecordQueryPlan removeIndexFetch(@Nonnull RecordMetaData recordMetaData,
                                                   @Nonnull PlannableIndexTypes indexTypes,
                                                   @Nullable KeyExpression commonPrimaryKey,
                                                   @Nonnull RecordQueryPlan plan,
                                                   @Nonnull Set<KeyExpression> requiredFields) {
        if (plan instanceof RecordQueryPlanWithIndex) {
            RecordQueryPlanWithIndex indexPlan = (RecordQueryPlanWithIndex) plan;
            Index index = recordMetaData.getIndex(indexPlan.getIndexName());

            final Collection<RecordType> recordTypes = recordMetaData.recordTypesForIndex(index);
            if (recordTypes.size() != 1) {
                return null;
            }
            final RecordType recordType = Iterables.getOnlyElement(recordTypes);
            AvailableFields fieldsFromIndex = AvailableFields.fromIndex(recordType, index, indexTypes, commonPrimaryKey);


            Set<KeyExpression> fields = new HashSet<>(requiredFields);
            if (commonPrimaryKey != null) {
                // Need the primary key, even if it wasn't one of the explicit result fields.
                fields.addAll(commonPrimaryKey.normalizeKeyForPositions());
            }

            if (fieldsFromIndex.containsAll(fields)) {
                final IndexKeyValueToPartialRecord keyValueToPartialRecord = fieldsFromIndex.buildIndexKeyValueToPartialRecord(recordType).build();
                if (keyValueToPartialRecord != null) {
                    return new RecordQueryCoveringIndexPlan(indexPlan, recordType.getName(), fieldsFromIndex, keyValueToPartialRecord);
                }
            }
        } else if (plan instanceof RecordQueryFetchFromPartialRecordPlan) {
            RecordQueryFetchFromPartialRecordPlan fetchPlan = (RecordQueryFetchFromPartialRecordPlan) plan;
            if (fetchPlan.getChild().getAvailableFields().containsAll(requiredFields)) {
                return ((RecordQueryFetchFromPartialRecordPlan)plan).getChild();
            }
        }
        return null;
    }
}

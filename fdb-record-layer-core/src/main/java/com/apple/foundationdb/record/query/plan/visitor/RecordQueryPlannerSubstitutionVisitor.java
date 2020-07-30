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
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithRequiredFields;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * Visitor interface for performing substitution-type rules on {@link RecordQueryPlan}s.
 */
public abstract class RecordQueryPlannerSubstitutionVisitor {
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
    public Set<KeyExpression> preVisitForRequiredFields(@Nonnull RecordQueryPlan plan, @Nonnull Set<KeyExpression> requiredFields) {
        if (plan instanceof RecordQueryPlanWithRequiredFields) {
            Set<KeyExpression> allRequiredFields = new HashSet<>(requiredFields);
            allRequiredFields.addAll(((RecordQueryPlanWithRequiredFields)plan).getRequiredFields());
            return allRequiredFields;
        } else {
            return requiredFields;
        }
    }

    @Nonnull
    public abstract RecordQueryPlan postVisit(@Nonnull RecordQueryPlan recordQueryPlan, @Nonnull Set<KeyExpression> requiredFields);

    @Nullable
    protected RecordQueryPlan removeIndexFetch(@Nonnull RecordQueryPlan plan, @Nonnull Set<KeyExpression> requiredFields) {
        if (plan instanceof RecordQueryPlanWithIndex) {
            RecordQueryPlanWithIndex indexPlan = (RecordQueryPlanWithIndex) plan;
            Index index = recordMetadata.getIndex(indexPlan.getIndexName());
            final IndexKeyValueToPartialRecord keyValueToPartialRecord = RecordQueryPlanner.buildIndexKeyValueToPartialRecord(recordMetadata, index, commonPrimaryKey, requiredFields);
            if (keyValueToPartialRecord != null) {
                @Nullable RecordType recordType = keyValueToPartialRecord.getRecordType();
                if (recordType != null) {
                    return new RecordQueryCoveringIndexPlan(indexPlan, recordType.getName(), keyValueToPartialRecord);
                }
            }
        } else if (plan instanceof RecordQueryFetchFromPartialRecordPlan) {
            return ((RecordQueryFetchFromPartialRecordPlan)plan).getChild();
        }
        return null;
    }
}

/*
 * UnionVisitor.java
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
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;

import java.util.stream.Collectors;

/**
 *
 * This visitor pulls index fetches after the intersection if possible.
 *
 *                                  Starting Plan
 *
 *                                      UnionPlan
 *                                  /              \
 *                                 /                \
 *                 RecordQueryPlan (IndexFetch) RecordQueryPlan (IndexFetch)
 *
 *                                          |
 *                                          |
 *                                          V
 *
 *                                  Transformed Plan
 *
 *                                  UnionPlan (IndexFetch)
 *                                  /              \
 *                                 /                \
 *                         RecordQueryPlan   RecordQueryPlan
 *
 */
public class UnionVisitor extends BaseMetadataVisitor {

    public UnionVisitor(RecordMetaData recordMetaData) {
        super(recordMetaData);
    }

    @Override
    public Visitable visit(final Visitable recordQueryPlan, final Visitable parentRecordQueryPlan) {
        if (recordQueryPlan instanceof RecordQueryUnionPlanBase) {
            RecordQueryUnionPlanBase unionPlan = (RecordQueryUnionPlanBase) recordQueryPlan;
            if (unionPlan.getChildren().stream().allMatch(plan -> isLookupMoveable(plan))) {
                unionPlan.getChildren().forEach(plan -> plan.setIsIndexFetch(false));
                if (parentRecordQueryPlan!= null && parentRecordQueryPlan instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan) {
                    unionPlan.setIsIndexFetch(false);
                    ((RecordQueryUnorderedPrimaryKeyDistinctPlan)parentRecordQueryPlan).setIsIndexFetch(true);
                } else {
                    unionPlan.setIsIndexFetch(true);
                }
            }
            if (unionPlan.getChildren().stream().allMatch(plan -> plan instanceof RecordQueryFilterPlan)) {
                QueryComponent filter = ((RecordQueryFilterPlan) unionPlan.getChildren().get(0)).getFilter();
                if (unionPlan.getChildren().stream().allMatch( plan -> ((RecordQueryFilterPlan) plan).getFilter().equals(filter) && ((RecordQueryFilterPlan)plan).getChild().isIndexFetch())) {
                    if (parentRecordQueryPlan == null) {
                        unionPlan.setChildren(unionPlan.getChildren().stream().map( plan -> {
                            RecordQueryPlan newChild = ((RecordQueryFilterPlan) plan).getChild();
                            newChild.setIsIndexFetch(false);
                            return newChild;
                        }).collect(Collectors.toList()));
                        unionPlan.setIsIndexFetch(true);
                        RecordQueryFilterPlan filterPlan = new RecordQueryFilterPlan(unionPlan, filter);
                        return filterPlan;
                    } else if (parentRecordQueryPlan instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan) {
                        unionPlan.setChildren(unionPlan.getChildren().stream().map( plan -> {
                            RecordQueryPlan newChild = ((RecordQueryFilterPlan) plan).getChild();
                            newChild.setIsIndexFetch(false);
                            return newChild;
                        }).collect(Collectors.toList()));
                        unionPlan.setIsIndexFetch(true);
                        RecordQueryFilterPlan filterPlan = new RecordQueryFilterPlan(unionPlan, filter);
                        ((RecordQueryUnorderedPrimaryKeyDistinctPlan) parentRecordQueryPlan).setChild(filterPlan);
                        return filterPlan;
                    }
                }
            }
        }
        return recordQueryPlan;
    }
}

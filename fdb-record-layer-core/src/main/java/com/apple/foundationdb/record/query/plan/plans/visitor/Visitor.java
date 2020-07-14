/*
 * Visitor.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

/**
 *
 * Visitor interface that allows developers to write a
 *
 */
public interface Visitor {

    static RecordQueryPlan applyVisitors(RecordQueryPlan plan, RecordMetaData recordMetaData) {
        return (RecordQueryPlan) plan
                .accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData))
                .accept(new UnionVisitor(recordMetaData))
                .accept(new IntersectionVisitor(recordMetaData))
                .accept(new UnorderedPrimaryKeyDistinctVisitor(recordMetaData))
                ;
    }

    Visitable visit(Visitable recordQueryPlan, Visitable parentRecordQueryPlan);

    default boolean stopTraversal() {
        return false;
    }

    default boolean skipChildren(Visitable recordQueryPlan) {
        return false;
    }

    default boolean visitChildrenFirst(Visitable recordQueryPlan) {
        return false;
    }
}

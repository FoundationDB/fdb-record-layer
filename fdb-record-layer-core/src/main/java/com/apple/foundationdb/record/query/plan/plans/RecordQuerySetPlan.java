/*
 * RecordQuerySetPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * Interface for query plans that represent set-based operators such as union or intersection.
 */
public interface RecordQuerySetPlan extends RecordQueryPlan {
    @Nonnull
    Set<KeyExpression> getRequiredFields();

    /**
     * Method that returns a list of values that are required to be evaluable by this set-based plan operator. These
     * values are evaluated by the plan operator during execution time and are usually comprised of values that e.g.
     * establish equality between records. This method is declarative in nature and is called by the planner in order
     * to evaluate optimized plan alternatives.
     *
     * @param baseAlias the base alias to use for all external references. This is the alias of the data stream
     *        the values can be evaluated over.
     * @return a list of values where each value is required to be evaluable by the set base operation
     */
    @Nonnull
    default List<? extends Value> getRequiredValues(@Nonnull final CorrelationIdentifier baseAlias) {
        return getRequiredFields()
                .stream()
                .map(keyExpression -> new ScalarTranslationVisitor(keyExpression).toResultValue(baseAlias))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Method to create a new set-based plan operator that mirrors the attributes of {@code this} except its children
     * which are replaced with new children. It is the responsibility of the caller to ensure that the newly created plan
     * operator is consistent with the new children. For instance, it is not advised to recreate this plan with a
     * list of children of different size.
     *
     * @param newChildren a list of new children
     * @return a new set-based plan
     */
    @Nonnull
    RecordQuerySetPlan withChildren(@Nonnull final List<? extends RecordQueryPlan> newChildren);
}

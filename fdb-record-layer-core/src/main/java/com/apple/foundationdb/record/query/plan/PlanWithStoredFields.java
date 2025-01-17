/*
 * PlanWithStoredFields.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A plan that has some, but not all, stored fields.
 *
 * Used by {@link AvailableFields#fromIndex} for an index type that appears in {@link PlannableIndexTypes#getUnstoredNonPrimaryKeyTypes()}
 * to determine which of the fields found in the index's definition actually appear in its index entries.
 * By default, only the primary key and any grouping key are assumed to be there.
 *
 * Note that this is about the fields of the index entries scanned by the plan <em>before</em> it fetches the base record
 * (or not if the covering optimization is applied).
 * Contrast this with {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan#getAvailableFields},
 * which exposes the fields available from plan if run as is, that is, with the implied base record fetch.
 */
@API(API.Status.INTERNAL)
public interface PlanWithStoredFields {
    /**
     * Adjust the set of fields available from an index entry scanned by this plan.
     *
     * Initially called with {@code nonStoredFields} populated with all the fields of {@code keyFields} that are not part
     * of either the primary key or any grouping key. Removing from {@code nonStoredFields} means that an additional field <em>is</em>
     * part of the index entry. Adding to {@code nonStoredFields} means that one of those fields <em>is not</em> part of the index
     * entry (which is unlikely, since the primary key needs to be someplace and indxes are typically grouped by grouping keys).
     *
     * @param keyFields all of the fields of the index definition (root key expression), normalized and in order
     * @param nonStoredFields the fields that <em>are not</em> stored, that is, available in the index entry, even though they are part of the index definition
     * @param otherFields fields that come from the index entry in a special way
     */
    void getStoredFields(@Nonnull List<KeyExpression> keyFields, @Nonnull List<KeyExpression> nonStoredFields,
                         @Nonnull List<KeyExpression> otherFields);
}

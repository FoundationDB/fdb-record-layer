/*
 * QueryPredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface describing a predicate that can be evaluated on a {@link SourceEntry} (usually derived from a record).
 *
 * <p>
 * {@code QueryPredicate} is generally quite similar to {@link com.apple.foundationdb.record.query.expressions.QueryComponent}.
 * However, there is a key difference in how each interface evaluates the predicate against a given record:
 * </p>
 * <ul>
 *     <li>
 *         A {@link com.apple.foundationdb.record.query.expressions.QueryComponent} is evaluated on a
 *         {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecord} wrapping a Protobuf message.
 *         To evaluate predicates on nested records, a {@code QueryComponent} such as
 *         {@link com.apple.foundationdb.record.query.expressions.NestedField} or
 *         {@link com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent} will descend into the nested
 *         record.
 *     </li>
 *     <li>
 *         A {@code QueryPredicate} is evaluated on a {@link SourceEntry}, which maps
 *         {@link com.apple.foundationdb.record.query.plan.temp.view.Source}s to values. The predicate can be evaluated
 *         on a nested record by specifying a complex {@code Source}, such as
 *         {@link com.apple.foundationdb.record.query.plan.temp.view.RepeatedFieldSource}. All sources are evaluated
 *         to produce a stream of source entries before any predicates are evaluated.
 *     </li>
 * </ul>
 *
 * <p>
 * Concretely, the difference between {@code QueryPredicate}s and {@code QueryComponent}s is most easily seen in the
 * way that multiple predicates on repeated fields can be expressed. Any query predicate anywhere in a tree of
 * predicates can make use of any source. In contrast, each {@link com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent}
 * is a single iteration through the field's repeated values so the values obtained by that iteration are usable only
 * within that {@code OneOfThemWithComponent}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public interface QueryPredicate extends PlannerExpression, PlanHashable {
    @Nullable
    <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                     @Nonnull SourceEntry sourceEntry);
}

/*
 * PushMapThroughFetchRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.map;

/**
 * A rule that pushes a map expression containing a {@link Value} that can be evaluated on a partial record of keys and
 * values that were retrieved by an index scan underneath a {@link RecordQueryFetchFromPartialRecordPlan} in order to
 * eliminate the fetch altogether.
 * <br>
 * A map operation can be pushed if and only if the {@link Value}s leaves refer to only entities in the fetched full
 * record that can be translated to counterparts in the index record. The translation is specific to the kind of index and
 * therefore the kind of {@link com.apple.foundationdb.record.query.plan.cascades.MatchCandidate}.
 * <br>
 * As the map operation itself changes the shape of the resulting record in a way that the data flow above the map is
 * completely detached from the partial records coming from the pure covering index scan (or records coming from the
 * index scan AND fetch), it is not simply enough to push the map underneath the fetch and then continue. We actually
 * need to make sure that the fetch can be completely eliminated. That can only happen when the down stream data flow
 * only accesses fields of the index. That is ensured either if all fields accessed by the map are leaves in
 * their record structures OR if we know that partially contained sub-records that are referred to in the map are not
 * further accessed downstream in ways that would require fetching the actual record in the first place.
 * <br>
 * This rule defers to a <em>push function</em> to translate values in an appropriate way. See
 * {@link TranslateValueFunction} for details.
 * <br>
 * Example 1:
 * <pre>
 * {@code
 *   index i1: _.a, _.b
 *   SELECT q.a, q.b
 *   FROM FETCH(COVERING(ISCAN(i1)))
 * }
 * </pre>
 * can be transformed into
 * <pre>
 * {@code
 *   SELECT q.a, q.b
 *   FROM COVERING(ISCAN(i1))
 * }
 * </pre>
 * TODO the following is possible but not implemented 
 * <br>
 * Example 2: {@code b} is nested, containing {@code c}s and {@code d}s. Index only contains {@code c}s
 * <pre>
 * {@code
 *   index i1: _.a, _.b.c
 *   SELECT q.a, q.b
 *   FROM FETCH(COVERING(ISCAN(i1)))
 * }
 * </pre>
 * cannot be transformed into
 * <pre>
 * {@code
 *   SELECT q.a, q.b
 *   FROM COVERING(ISCAN(i1))
 * }
 * </pre>
 * as {@code q.b} is a partially populated sub-record as {@code i1} does not contain {@code d}s
 *
 * Example 3: {@code b} is nested, containing {@code c}s and {@code d}s. Index contains both {@code c}s and {@code d}s.
 * <pre>
 * {@code
 *   index i2: _.a, _.b.c, _.b.d
 *   SELECT q.a, q.b
 *   FROM FETCH(COVERING(ISCAN(i2)))
 * }
 * </pre>
 * can be transformed into
 * <pre>
 * {@code
 *   SELECT q.a, q.b
 *   FROM COVERING(ISCAN(i2))
 * }
 * </pre>
 * as {@code q.b} is a fully-populated sub-record as {@code i2} provides values for both {@code c}s and {@code d}s.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class PushMapThroughFetchRule extends ImplementationCascadesRule<RecordQueryMapPlan> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            RecordQueryPlanMatchers.fetchFromPartialRecordPlan(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            physicalQuantifier(fetchPlanMatcher);
    @Nonnull
    private static final BindingMatcher<RecordQueryMapPlan> root =
            map(quantifierOverFetchMatcher);

    public PushMapThroughFetchRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQueryMapPlan mapPlan = bindings.get(root);
        final RecordQueryFetchFromPartialRecordPlan fetchPlan = bindings.get(fetchPlanMatcher);
        final Quantifier.Physical quantifierOverFetch = bindings.get(quantifierOverFetchMatcher);
        final RecordQueryPlan innerPlan = bindings.get(innerPlanMatcher);

        // Note that physical operators are already simplified
        final var resultValue = mapPlan.getResultValue();

        // try to push these field values
        final CorrelationIdentifier newInnerAlias = Quantifier.uniqueId();
        final var pushedResultValueOptional =
                fetchPlan.pushValue(resultValue, quantifierOverFetch.getAlias(), newInnerAlias);
        if (pushedResultValueOptional.isEmpty()) {
            return;
        }
        final Quantifier.Physical newInnerQuantifier = Quantifier.physical(call.memoizePlan(innerPlan), newInnerAlias);

        // construct a new map plan that ranges over the plan the fetch ranges over
        final var pushedMapPlan =
                new RecordQueryMapPlan(newInnerQuantifier, pushedResultValueOptional.get());

        // effectively throw away the fetch
        call.yieldPlan(pushedMapPlan);
    }
}

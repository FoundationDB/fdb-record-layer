/*
 * QuantifiedValuePredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PQuantifiedValuePredicate;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public class QuantifiedValuePredicate extends ValuePredicate {

    public QuantifiedValuePredicate(@Nonnull final Value value, @Nonnull final Comparison comparison) {
        super(value, comparison);
        Verify.verify(value instanceof QuantifiedObjectValue);
    }

    private QuantifiedValuePredicate(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PQuantifiedValuePredicate proto) {
        super(Value.fromValueProto(serializationContext, Objects.requireNonNull(proto.getSuper().getValue())),
                Comparison.fromComparisonProto(serializationContext, Objects.requireNonNull(proto.getSuper().getComparison())));
    }

    @Nonnull
    public CorrelationIdentifier getQuantifierAlias() {
        return ((QuantifiedObjectValue) getValue()).getAlias();
    }

    @Nonnull
    @Override
    public PredicateMultiMap.PredicateCompensationFunction computeCompensationFunction(@Nonnull final PartialMatch partialMatch,
                                                                                       @Nonnull final QueryPredicate originalQueryPredicate,
                                                                                       @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                       @Nonnull final List<PredicateMultiMap.PredicateCompensationFunction> childrenResults,
                                                                                       @Nonnull final PullUp pullUp) {
        Verify.verify(childrenResults.isEmpty());
        Verify.verify(originalQueryPredicate instanceof QuantifiedValuePredicate);
        final var originalQuantifiedQueryPredicate = (QuantifiedValuePredicate) originalQueryPredicate;
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var matchesAnyExistentialQuantifier = partialMatch.getQueryExpression().getQuantifiers().stream()
                .anyMatch(quantifier -> quantifier.getAlias().equals(originalQuantifiedQueryPredicate.getQuantifierAlias()));
        if (matchesAnyExistentialQuantifier) {
            final var childPartialMatchOptional = regularMatchInfo.getChildPartialMatchMaybe(originalQuantifiedQueryPredicate.getQuantifierAlias());
            final var compensationOptional =
                    childPartialMatchOptional.map(childPartialMatch ->
                            childPartialMatch.compensateExistential(boundParameterPrefixMap));
            if (compensationOptional.isEmpty() || compensationOptional.get().isNeededForFiltering()) {
                return PredicateMultiMap.PredicateCompensationFunction.ofExistentialValuePredicate(originalQuantifiedQueryPredicate);
            }
        }
        return PredicateMultiMap.PredicateCompensationFunction.noCompensationNeeded();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public QueryPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues) {
        final var translatedValue = getValue().translateCorrelations(translationMap, shouldSimplifyValues);
        if (getValue() != translatedValue) {
            return new QuantifiedValuePredicate(translatedValue, getComparison());
        }
        return this;
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder()
                .setQuantifiedValuePredicate(PQuantifiedValuePredicate.newBuilder()
                        .setSuper(toProto(serializationContext))
                        .build())
                .build();
    }

    @Nonnull
    public static QuantifiedValuePredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PQuantifiedValuePredicate proto) {
        return new QuantifiedValuePredicate(serializationContext, proto);
    }


    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PQuantifiedValuePredicate, QuantifiedValuePredicate> {
        @Nonnull
        @Override
        public Class<PQuantifiedValuePredicate> getProtoMessageClass() {
            return PQuantifiedValuePredicate.class;
        }

        @Nonnull
        @Override
        public QuantifiedValuePredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PQuantifiedValuePredicate proto) {
            return QuantifiedValuePredicate.fromProto(serializationContext, proto);
        }
    }
}

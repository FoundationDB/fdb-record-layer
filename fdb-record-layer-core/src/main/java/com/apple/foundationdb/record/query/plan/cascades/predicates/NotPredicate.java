/*
 * NotPredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PNotPredicate;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link QueryPredicate} that is satisfied when its child component is not satisfied.
 * <br>
 * For tri-valued logic, if the child evaluates to unknown / {@code null}, {@code NOT} is still unknown.
 */
@API(API.Status.EXPERIMENTAL)
public class NotPredicate extends AbstractQueryPredicate implements QueryPredicateWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Predicate");

    @Nonnull
    public final QueryPredicate child;

    private NotPredicate(@Nonnull final PlanSerializationContext serializationContext,
                         @Nonnull final PNotPredicate notPredicateProto) {
        super(serializationContext, Objects.requireNonNull(notPredicateProto.getSuper()));
        this.child = QueryPredicate.fromQueryPredicateProto(serializationContext, Objects.requireNonNull(notPredicateProto.getChild()));
    }

    private NotPredicate(@Nonnull final QueryPredicate child, final boolean isAtomic) {
        super(isAtomic);
        this.child = child;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        return invert(child.eval(store, context));
    }

    @Nullable
    private Boolean invert(@Nullable Boolean v) {
        if (v == null) {
            return null;
        } else {
            return !v;
        }
    }

    @Nonnull
    @Override
    public QueryPredicate getChild() {
        return child;
    }

    @Override
    public String toString() {
        return "Not(" + getChild() + ")";
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return Objects.hash(hashCodeWithoutChildren(), getChild());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), super.hashCodeWithoutChildren());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getChild().planHash(mode) + 1;
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public NotPredicate withChild(@Nonnull final QueryPredicate newChild) {
        return new NotPredicate(newChild, isAtomic());
    }

    @Nonnull
    @Override
    public Optional<PredicateMultiMap.ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                                                  @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                                                  @Nonnull final List<Optional<PredicateMultiMap.ExpandCompensationFunction>> childrenResults) {
        Verify.verify(childrenResults.size() == 1);
        final var childInjectCompensationFunctionOptional = Iterables.getOnlyElement(childrenResults);
        if (childInjectCompensationFunctionOptional.isEmpty()) {
            return Optional.empty();
        }
        final var childInjectCompensationFunction = childInjectCompensationFunctionOptional.get();

        return Optional.of(translationMap -> {
            final var childPredicates = childInjectCompensationFunction.applyCompensationForPredicate(translationMap);
            return LinkedIdentitySet.of(not(AndPredicate.and(childPredicates)));
        });
    }

    @Nonnull
    @Override
    public NotPredicate withAtomicity(final boolean isAtomic) {
        return new NotPredicate(getChild(), isAtomic);
    }

    @Nonnull
    @Override
    public PNotPredicate toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PNotPredicate.newBuilder()
                .setSuper(toAbstractQueryPredicateProto(serializationContext))
                .setChild(child.toQueryPredicateProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PQueryPredicate toQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setNotPredicate(toProto(serializationContext)).build();
    }

    @Nonnull
    public static NotPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PNotPredicate notPredicateProto) {
        return new NotPredicate(serializationContext, notPredicateProto);
    }

    @Nonnull
    public static NotPredicate not(@Nonnull final QueryPredicate predicate) {
        return of(predicate, false);
    }

    @Nonnull
    public static NotPredicate of(@Nonnull final QueryPredicate predicate, final boolean isAtomic) {
        return new NotPredicate(predicate, isAtomic);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PNotPredicate, NotPredicate> {
        @Nonnull
        @Override
        public Class<PNotPredicate> getProtoMessageClass() {
            return PNotPredicate.class;
        }

        @Nonnull
        @Override
        public NotPredicate fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PNotPredicate notPredicateProto) {
            return NotPredicate.fromProto(serializationContext, notPredicateProto);
        }
    }
}

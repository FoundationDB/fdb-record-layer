/*
 * NotComponent.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link QueryComponent} that is satisfied when its child component is not satisfied.
 * <br>
 * For tri-valued logic, if the child evaluates to unknown / {@code null}, {@code NOT} is still unknown.
 */
@API(API.Status.UNSTABLE)
public class NotComponent implements ComponentWithSingleChild, BooleanComponent {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Not-Component");

    private final QueryComponent child;

    public NotComponent(QueryComponent child) {
        this.child = child;
    }

    @Nullable
    private Boolean invert(@Nullable Boolean v) {
        if (v == null) {
            return null;
        } else {
            return !v;
        }
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(FDBRecordStoreBase<M> store, EvaluationContext context,
                                                   @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return invert(getChild().evalMessage(store, context, rec, message));
    }

    @Override
    public <M extends Message> CompletableFuture<Boolean> evalMessageAsync(FDBRecordStoreBase<M> store, EvaluationContext context,
                                                                           @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return getChild().evalMessageAsync(store, context, rec, message).thenApply(this::invert);
    }

    @Override
    public boolean isAsync() {
        return getChild().isAsync();
    }

    @Override
    public void validate(Descriptors.Descriptor descriptor) {
        getChild().validate(descriptor);
    }

    @Override
    public String toString() {
        return "Not(" + getChild() + ")";
    }

    /**
     * Child for this component.
     */
    @Override
    public QueryComponent getChild() {
        return child;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public QueryComponent withOtherChild(QueryComponent newChild) {
        if (newChild == getChild()) {
            return this;
        }
        return new NotComponent(newChild);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NotComponent not = (NotComponent) o;
        return Objects.equals(getChild(), not.getChild());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChild());
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getChild().planHash(mode) + 1;
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, getChild());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public GraphExpansion expand(final Quantifier.ForEach baseQuantifier,
                                 final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 final List<String> fieldNamePrefix) {
        final GraphExpansion childGraphExpansion = child.expand(baseQuantifier, outerQuantifierSupplier, fieldNamePrefix);
        return childGraphExpansion.withPredicate(NotPredicate.not(childGraphExpansion.asAndPredicate()));
    }
}

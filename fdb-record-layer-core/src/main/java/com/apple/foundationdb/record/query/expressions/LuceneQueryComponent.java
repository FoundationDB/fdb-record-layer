/*
 * FullTextQueryComponent.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GraphExpansion;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.common.base.Verify;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import jdk.jfr.Experimental;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A Query Component for Lucene that wraps the query supplied.
 *
 */
@Experimental
public class LuceneQueryComponent implements QueryComponent, ComponentWithComparison, ComponentWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Query");

    private final Comparisons.LuceneComparison comparison;

    public LuceneQueryComponent(String query) {
        this(new Comparisons.LuceneComparison(query));
    }

    private LuceneQueryComponent(Comparisons.LuceneComparison comparison) {
        this.comparison = comparison;
    }

    @Nonnull
    @Override
    public <M extends Message> Boolean evalMessage(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final Message message) {
        throw new RecordCoreException("Multiple lucene components are not yet supported");
    }

    @Override
    public void validate(@Nonnull final Descriptors.Descriptor descriptor) {
        // No-op
    }

    @Override
    public GraphExpansion expand(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        return null;
    }

    @Nonnull
    @Override
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public QueryComponent withOtherComparison(final Comparisons.Comparison comparison) {
        Verify.verify(comparison instanceof Comparisons.LuceneComparison);
        return new LuceneQueryComponent((Comparisons.LuceneComparison)comparison);
    }

    @Override
    @Nonnull
    public String getName() {
        return "LuceneQuery";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LuceneQueryComponent)) {
            return false;
        }
        final LuceneQueryComponent that = (LuceneQueryComponent)o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return comparison.planHash(hashKind);
    }

    @Override
    @Nonnull
    public String toString() {
        return "LuceneQuery(" + comparison.getComparand() + ")";
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, comparison.getComparand());
    }
}

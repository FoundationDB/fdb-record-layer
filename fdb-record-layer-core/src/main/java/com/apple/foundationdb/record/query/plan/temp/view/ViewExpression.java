/*
 * ViewExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An abstraction that represents a stream of values that can be represented as tuple-encoding keys and binary values
 * in FoundationDB.
 *
 * <p>
 * Visually, a view expression is quite similar to a query in a declarative language like SQL. A view expression is
 * composed of several clauses, including:
 * </p>
 * <ul>
 *     <li>A root {@link Source}, which defines the stream of values from which the expression's {@link Element}s are generated.</li>
 *     <li>A projection, which is a collection of {@link Element}s that generate values.</li>
 *     <li>
 *         A sort order, which is an ordered list of {@link Element}s that define the order of the stream. By definition,
 *         the sort order is the lexicographic order of the elements when packed together as tuple.
 *       </li>
 *     <li>A predicate, in the form of a {@link QueryPredicate}</li>
 * </ul>
 *
 * <p>
 * Because view expressions are quite general but are closely tied to the semantics of the tuple layer, they are very
 * useful for planning tasks such as incremental index selection. To see how an index is represented as a view
 * expression, see {@link #fromIndexDefinition(String, Collection, KeyExpression)}.
 * </p>
 */
public class ViewExpression {
    @Nonnull
    private final List<Element> select;
    @Nonnull
    private final Source rootSource;
    @Nonnull
    private final List<Element> orderBy;

    // Note that predicate is null when used for index selection. It is included for completeness since future uses
    // of ViewExpression (e.g., as a query intermediate representation for queries with joins) will likely use it.
    @Nullable
    private final QueryPredicate predicate;

    public ViewExpression(@Nonnull List<Element> select, @Nullable QueryPredicate predicate, @Nonnull Source rootSource, @Nonnull List<Element> orderBy) {
        this.select = select;
        this.predicate = predicate;
        this.rootSource = rootSource;
        this.orderBy = orderBy;
    }

    @Nonnull
    public List<Element> getOrderBy() {
        return orderBy;
    }

    /**
     * Generate a view expression that represents the structure of an index defined using a {@link KeyExpression}.
     * @param indexType the type of the index as specified in the meta-data
     * @param recordTypes the set of record types used to generate the index
     * @param rootExpression the root key expression of the index as specified in the meta-data
     * @return a view expression representing the given index structure
     */
    public static ViewExpression fromIndexDefinition(@Nonnull String indexType,
                                                     @Nonnull Collection<String> recordTypes,
                                                     @Nonnull KeyExpression rootExpression) {
        ViewExpression.Builder builder = ViewExpression.builder();
        if (indexType.equals(IndexTypes.VALUE)) {
            for (String recordType : recordTypes) {
                builder.addRecordType(recordType);
            }

            final KeyExpression normalizedForPlanner = rootExpression.normalizeForPlannerOld(builder.buildBaseSource(),
                    Collections.emptyList());

            if (normalizedForPlanner instanceof KeyWithValueExpression) { // Handle covering indexes.
                final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression) normalizedForPlanner;
                keyWithValueExpression.getKeyExpression()
                        .flattenForPlannerOld()
                        .forEach(builder::addTupleElement);
                keyWithValueExpression.getValueExpression()
                        .flattenForPlannerOld()
                        .forEach(builder::addSelectElement);
                // TODO add other branches to handle other special cases, such as grouping key expressions.
            } else {
                normalizedForPlanner.flattenForPlannerOld().forEach(builder::addTupleElement);
            }
        }

        return builder.build();
    }

    /**
     * Return a copy of this view expression with all occurrences of the duplicate source replaced with the original
     * source.
     * TODO Does not map the predicate yet!
     * @param originalSource a source to replace all occurrences of the duplicate source with
     * @param duplicateSource a source to replace with the original source
     * @return a copy of this view expression with all occurrences of the duplicate source replaced with the original source
     */
    ViewExpression withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        final ImmutableList.Builder<Element> mappedSelect = ImmutableList.builder();
        for (Element element : select) {
            mappedSelect.add(element.withSourceMappedInto(originalSource, duplicateSource));
        }
        final ImmutableList.Builder<Element> mappedOrderBy = ImmutableList.builder();
        for (Element element : orderBy) {
            mappedOrderBy.add(element.withSourceMappedInto(originalSource, duplicateSource));
        }
        return new ViewExpression(mappedSelect.build(),
                predicate, // TODO map the predicate too
                rootSource.withSourceMappedInto(originalSource, duplicateSource),
                mappedOrderBy.build());
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing an (immutable) view expression.
     */
    public static class Builder {
        @Nonnull
        private final ImmutableList.Builder<Element> select;
        @Nonnull
        private final ImmutableSet.Builder<String> recordTypeNames;
        @Nullable
        private Source rootSource = null;
        @Nonnull
        private final ImmutableList.Builder<Element> orderBy;

        private Builder() {
            this.select = ImmutableList.builder();
            this.recordTypeNames = ImmutableSet.builder();
            this.orderBy = ImmutableList.builder();
        }

        public Builder setRootSource(@Nonnull Source rootSource) {
            this.rootSource = rootSource;
            return this;
        }

        @Nonnull
        public Source buildBaseSource() {
            rootSource = new RecordTypeSource(recordTypeNames.build());
            return rootSource;
        }

        @Nonnull
        public Quantifier buildBase() {
            return Quantifier.forEach(GroupExpressionRef.of(
                    new FullUnorderedScanExpression(recordTypeNames.build())));
        }

        public Builder addRecordType(@Nonnull String recordTypeName) {
            recordTypeNames.add(recordTypeName);
            return this;
        }

        public Builder addTupleElement(@Nonnull Element element) {
            select.add(element);
            orderBy.add(element);
            return this;
        }

        public Builder addSelectElement(@Nonnull Element element) {
            select.add(element);
            return this;
        }

        public ViewExpression build() {
            if (rootSource == null) {
                buildBaseSource();
            }
            return new ViewExpression(select.build(), null, rootSource, orderBy.build());
        }
    }
}

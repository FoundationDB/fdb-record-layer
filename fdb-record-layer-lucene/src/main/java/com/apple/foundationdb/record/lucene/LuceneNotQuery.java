/*
 * LuceneNotQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Binder for a negation of clauses.
 * Because of the way Lucene {@link BooleanQuery} works, this actually represents set subtraction,
 * with a set of positive and negative clauses. For the same reason, there is no disjunctive analogue.
 */
@API(API.Status.UNSTABLE)
public class LuceneNotQuery extends LuceneBooleanQuery {
    @Nonnull
    private final List<LuceneQueryClause> negatedChildren;

    public LuceneNotQuery(@Nonnull final LuceneQueryType queryType,
                          @Nonnull final List<LuceneQueryClause> children,
                          @Nonnull final List<LuceneQueryClause> negatedChildren) {
        super(queryType, children, BooleanClause.Occur.MUST);
        this.negatedChildren = negatedChildren;
    }

    public LuceneNotQuery(@Nonnull final LuceneQueryType queryType, @Nonnull final LuceneQueryClause negatedChild) {
        this(queryType, Collections.emptyList(), Collections.singletonList(negatedChild));
    }

    @Nonnull
    protected List<LuceneQueryClause> getNegatedChildren() {
        return negatedChildren;
    }

    @Override
    public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Map<String, Set<String>> highlightingTermsMap = null;
        if (getChildren().isEmpty()) {
            // Lucene cannot handle all negated clauses.
            final MatchAllDocsQuery matchAllDocsQuery = new MatchAllDocsQuery();
            builder.add(matchAllDocsQuery, BooleanClause.Occur.MUST);
            if (getQueryType() == LuceneQueryType.QUERY_HIGHLIGHT) {
                highlightingTermsMap = toBoundQuery(matchAllDocsQuery).getHighlightingTermsMap();
            }
        } else {
            for (final LuceneQueryClause child : getChildren()) {
                final BoundQuery childBoundQuery = child.timedBind(store, index, context);
                builder.add(childBoundQuery.getLuceneQuery(), BooleanClause.Occur.MUST);
                final Map<String, Set<String>> childHighlightingTermsMap = childBoundQuery.getHighlightingTermsMap();
                if (childHighlightingTermsMap != null) {
                    if (highlightingTermsMap == null) {
                        highlightingTermsMap = Maps.newHashMap();
                    }
                    combineHighlightingTermsMaps(highlightingTermsMap, childHighlightingTermsMap);
                }
            }
        }
        for (final LuceneQueryClause child : negatedChildren) {
            final BoundQuery childBoundQuery = child.timedBind(store, index, context);
            builder.add(childBoundQuery.getLuceneQuery(), BooleanClause.Occur.MUST_NOT);
            final Map<String, Set<String>> childHighlightingTermsMap = childBoundQuery.getHighlightingTermsMap();
            if (childHighlightingTermsMap != null) {
                if (highlightingTermsMap == null) {
                    highlightingTermsMap = Maps.newHashMap();
                }
                combineHighlightingTermsMaps(highlightingTermsMap, childHighlightingTermsMap);
            }
        }
        return new BoundQuery(builder.build(), highlightingTermsMap);
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        super.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        for (LuceneQueryClause child : negatedChildren) {
            child.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return super.planHash(mode) - PlanHashable.iterablePlanHash(mode, negatedChildren);
    }

    @Override
    public String toString() {
        return Stream.concat(
                getChildren().stream().map(Objects::toString),
                negatedChildren.stream().map(c -> "NOT " + c)
        ).collect(Collectors.joining(" AND "));
    }
}

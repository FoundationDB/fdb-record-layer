/*
 * LuceneBooleanQuery.java
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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Binder for a conjunction of other clauses.
 */
@API(API.Status.UNSTABLE)
public class LuceneBooleanQuery extends LuceneQueryClause {
    @Nonnull
    private final List<LuceneQueryClause> children;
    @Nonnull
    private final BooleanClause.Occur occur;

    public LuceneBooleanQuery(@Nonnull LuceneQueryType queryType, @Nonnull List<LuceneQueryClause> children, @Nonnull BooleanClause.Occur occur) {
        super(queryType);
        this.children = children;
        this.occur = occur;
    }

    @Nonnull
    protected List<LuceneQueryClause> getChildren() {
        return children;
    }

    @Nonnull
    protected BooleanClause.Occur getOccur() {
        return occur;
    }

    @Override
    public BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Map<String, Set<String>> highlightingTermsMap = null;
        for (LuceneQueryClause child : children) {
            final BoundQuery childBoundQuery = child.bind(store, index, context);
            builder.add(childBoundQuery.getLuceneQuery(), occur);
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
        detailsBuilder.add("occur: {{occur}}");
        attributeMapBuilder.put("occur", Attribute.gml(occur));
        for (LuceneQueryClause child : children) {
            child.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.iterablePlanHash(mode, children);
    }

    @Override
    public String toString() {
        final String op;
        switch (occur) {
            case MUST:
                op = " AND ";
                break;
            case SHOULD:
                op = " OR ";
                break;
            default:
                op = " " + occur.name() + " ";
                break;
        }
        return children.stream().map(Objects::toString).collect(Collectors.joining(op));
    }
}

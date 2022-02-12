/*
 * LuceneBooleanQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
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

    public LuceneBooleanQuery(@Nonnull List<LuceneQueryClause> children, @Nonnull BooleanClause.Occur occur) {
        this.children = children;
        this.occur = occur;
    }

    @Override
    public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (LuceneQueryClause child : children) {
            builder.add(child.bind(store, index, context), occur);
        }
        return builder.build();
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
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.iterablePlanHash(hashKind, children);
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

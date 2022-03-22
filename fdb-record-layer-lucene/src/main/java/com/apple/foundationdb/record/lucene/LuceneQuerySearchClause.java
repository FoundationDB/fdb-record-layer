/*
 * LuceneQuerySearchClause.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;

/**
 * Query clause from string using Lucene search syntax.
 */
@API(API.Status.UNSTABLE)
public class LuceneQuerySearchClause extends LuceneQueryClause {
    @Nonnull
    private final String defaultField;
    @Nonnull
    private final String search;
    private final boolean isParameter;

    // TODO: Need better predicates for controlling field.
    public LuceneQuerySearchClause(@Nonnull final String search, final boolean isParameter) {
        this(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, search, isParameter);
    }

    public LuceneQuerySearchClause(@Nonnull final String defaultField, @Nonnull final String search, final boolean isParameter) {
        this.defaultField = defaultField;
        this.search = search;
        this.isParameter = isParameter;
    }

    @Nonnull
    public String getDefaultField() {
        return defaultField;
    }

    @Nonnull
    public String getSearch() {
        return search;
    }

    public boolean isParameter() {
        return isParameter;
    }

    @Override
    public Query bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        final Pair<Analyzer, Analyzer> analyzerPair = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerPair(index);
        final QueryParser parser = new QueryParser(defaultField, analyzerPair.getRight());
        final String searchString = isParameter ? (String)context.getBinding(search) : search;
        try {
            return parser.parse(searchString);
        } catch (Exception ioe) {
            throw new RecordCoreArgumentException("Unable to parse search given for query", ioe);
        }
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        if (!LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME.equals(defaultField)) {
            detailsBuilder.add("field: {{field}}");
            attributeMapBuilder.put("field", Attribute.gml(defaultField));
        }
        if (isParameter) {
            detailsBuilder.add("param: {{param}}");
            attributeMapBuilder.put("param", Attribute.gml(search));
        } else {
            detailsBuilder.add("search: {{search}}");
            attributeMapBuilder.put("search", Attribute.gml(search));
        }
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, defaultField, search, isParameter);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (!LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME.equals(defaultField)) {
            str.append(defaultField).append(":");
        }
        if (isParameter) {
            str.append("$");
        }
        str.append(search);
        return str.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LuceneQuerySearchClause that = (LuceneQuerySearchClause)o;

        if (isParameter != that.isParameter) {
            return false;
        }
        if (!defaultField.equals(that.defaultField)) {
            return false;
        }
        return search.equals(that.search);
    }

    @Override
    public int hashCode() {
        int result = defaultField.hashCode();
        result = 31 * result + search.hashCode();
        result = 31 * result + (isParameter ? 1 : 0);
        return result;
    }
}

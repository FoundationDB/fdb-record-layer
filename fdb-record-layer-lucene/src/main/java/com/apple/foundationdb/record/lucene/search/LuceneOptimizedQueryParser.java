/*
 * LuceneOptimizedQueryParser.java
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

package com.apple.foundationdb.record.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Optimized {@link QueryParser} that adds the slop for {@link SpanNearQuery} as well.
 * So the proximity search based on {@link SpanNearQuery} can also work.
 */
public class LuceneOptimizedQueryParser extends QueryParser implements ConfigAwareQueryParser {

    @Nonnull
    private final Map<String, PointsConfig> pointsConfig;

    public LuceneOptimizedQueryParser(String field, Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig) {
        super(field, analyzer);
        this.pointsConfig = pointsConfig;
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop)
            throws ParseException {
        Query query = getFieldQuery(field, queryText, true);

        return addSlop(query, slop);
    }

    @Override
    protected Query getFieldQuery(final String field, final String queryText, final boolean quoted) throws ParseException {
        return attemptConstructFieldQueryWithPointsConfig(field, queryText, quoted);
    }

    @Override
    protected Query getRangeQuery(final String field, final String part1, final String part2, final boolean startInclusive, final boolean endInclusive) throws ParseException {
        return attemptConstructRangeQueryWithPointsConfig(field, part1, part2, startInclusive, endInclusive);
    }

    @Nonnull
    @Override
    public Map<String, PointsConfig> getPointsConfig() {
        return pointsConfig;
    }

    @Nonnull
    @Override
    public Query constructFieldWithoutPointsConfig(final @Nonnull String field, final @Nonnull String queryText, final boolean quoted) throws ParseException {
        return super.getFieldQuery(field, queryText, quoted);
    }

    @Nonnull
    @Override
    public Query constructRangeQueryWithoutPointsConfig(final @Nonnull String field, final @Nonnull String part1, final @Nonnull String part2, final boolean startInclusive, final boolean endInclusive) throws ParseException {
        return getRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }

    @Nonnull
    @Override
    public final Token nextToken() {
        return getNextToken();
    }
}

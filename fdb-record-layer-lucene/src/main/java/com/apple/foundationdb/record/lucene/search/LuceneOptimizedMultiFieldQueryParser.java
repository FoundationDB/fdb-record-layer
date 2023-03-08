/*
 * LuceneOptimizedMultiFieldQueryParser.java
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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Optimized {@link MultiFieldQueryParser} that adds the slop for {@link SpanNearQuery} as well.
 * So the proximity search based on {@link SpanNearQuery} can also work.
 * <p>
 * Additionally, this parser will do type-specific query construction for numeric query types,
 * when the appropriate {@link PointsConfig} elements are set.
 */
public class LuceneOptimizedMultiFieldQueryParser extends MultiFieldQueryParser implements ConfigAwareQueryParser {

    @Nonnull
    private final Map<String, PointsConfig> pointsConfig;

    public LuceneOptimizedMultiFieldQueryParser(String[] fields, Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig) {
        super(fields, analyzer);
        this.pointsConfig = pointsConfig;
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
        if (field == null) {
            List<Query> clauses = new ArrayList<>();
            Map<String, Float> boostMap = boosts == null ? Collections.emptyMap() : boosts;
            for (final String s : fields) {
                Query q = super.getFieldQuery(s, queryText, true);
                if (q != null) {
                    //Get the boost from the map and apply them
                    Float boost = boostMap.get(s);
                    if (boost != null) {
                        q = new BoostQuery(q, boost.floatValue());
                    }
                    q = addSlop(q, slop);
                    clauses.add(q);
                }
            }
            if (clauses.isEmpty()) {  // happens for stopwords
                return null;
            }
            return getMultiFieldQuery(clauses);
        }
        Query q = super.getFieldQuery(field, queryText, true);
        q = addSlop(q, slop);
        return q;
    }


    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    protected Query getFieldQuery(final String field, final String queryText, final boolean quoted) throws ParseException {
        return attemptConstructFieldQueryWithPointsConfig(field, queryText, quoted);
    }


    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    protected Query getRangeQuery(final String field,
                                  final String part1,
                                  final String part2,
                                  final boolean startInclusive,
                                  final boolean endInclusive) throws ParseException {
        return attemptConstructRangeQueryWithPointsConfig(field, part1, part2, startInclusive, endInclusive);
    }


    @Nonnull
    @Override
    public Map<String, PointsConfig> getPointsConfig() {
        return pointsConfig;
    }

    @Nonnull
    @Override
    public Query constructFieldWithoutPointsConfig(final String field, final String queryText, final boolean quoted) throws ParseException {
        return super.getFieldQuery(field, queryText, quoted);
    }

    @Nonnull
    @Override
    public Query constructRangeQueryWithoutPointsConfig(final String field, final String part1, final String part2, final boolean startInclusive, final boolean endInclusive) throws ParseException {
        return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }
}

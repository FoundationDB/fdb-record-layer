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
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

import javax.annotation.Nonnull;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Optimized {@link MultiFieldQueryParser} that adds the slop for {@link SpanNearQuery} as well.
 * So the proximity search based on {@link SpanNearQuery} can also work.
 * <p>
 * Additionally, this parser will do type-specific query construction for numeric query types,
 * when the appropriate {@link PointsConfig} elements are set.
 */
public class LuceneOptimizedMultiFieldQueryParser extends MultiFieldQueryParser {

    private Map<String, PointsConfig> pointsConfig;

    public LuceneOptimizedMultiFieldQueryParser(String[] fields, Analyzer analyzer) {
        super(fields, analyzer);
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
        if (field == null) {
            List<Query> clauses = new ArrayList<>();
            for (int i = 0; i < fields.length; i++) {
                Query q = super.getFieldQuery(fields[i], queryText, true);
                if (q != null) {
                    //If the user passes a map of boosts
                    if (boosts != null) {
                        //Get the boost from the map and apply them
                        Float boost = boosts.get(fields[i]);
                        if (boost != null) {
                            q = new BoostQuery(q, boost.floatValue());
                        }
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
        PointsConfig cfg = pointsConfig.get(field);
        if (cfg == null) {
            return super.getFieldQuery(field, queryText, quoted);
        }
        //parse the text as the correct type and convert it to a query

        NumberFormat format = cfg.getNumberFormat();
        Number point;
        try {
            point = format.parse(queryText);
        } catch (java.text.ParseException pe) {
            throw new ParseException(QueryParserMessages.COULD_NOT_PARSE_NUMBER);
        }

        if (Integer.class.equals(cfg.getType())) {
            return IntPoint.newExactQuery(field, point.intValue());
        } else if (Long.class.equals(cfg.getType())) {
            return LongPoint.newExactQuery(field, point.longValue());
        } else if (Double.class.equals(cfg.getType())) {
            return DoublePoint.newExactQuery(field, point.doubleValue());
        } else if (Float.class.equals(cfg.getType())) {
            return FloatPoint.newExactQuery(field, point.floatValue());
        } else {
            throw new ParseException("Unknown numeric type: " + cfg.getType().getCanonicalName());
        }
    }


    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    protected Query getRangeQuery(final String field,
                                  final String part1,
                                  final String part2,
                                  final boolean startInclusive,
                                  final boolean endInclusive) throws ParseException {
        /*
         * Lucene doesn't really understand types, so unless we tell it that we are looking at numeric-valued
         * data points in our scan, it will parse everything as text, which results in incorrect results
         * being returned over range scans.
         *
         * To avoid this, we use a PointConfig map (an idea taken from Lucene's StandardAnalyzer). This allows
         * us to specify the data types for individual fields in Lucene, which we use here to create the correct
         * type of Query object for range scans.
         */
        PointsConfig cfg = pointsConfig.get(field);
        if (cfg == null) {
            return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
        } else {
            NumberFormat format = cfg.getNumberFormat();
            Number start;
            Number end;
            try {
                start = format.parse(part1);
                end = format.parse(part2);
            } catch (java.text.ParseException pe) {
                throw new ParseException(QueryParserMessages.COULD_NOT_PARSE_NUMBER);
            }

            if (Integer.class.equals(cfg.getType())) {
                int s = start.intValue();
                int e = end.intValue();
                //lucene range queries are inclusive, so adjust ranges as needed
                if (!startInclusive) {
                    s = Math.addExact(s, 1);
                }
                if (!endInclusive) {
                    e = Math.addExact(e, -1);
                }

                return IntPoint.newRangeQuery(field, s, e);
            } else if (Long.class.equals(cfg.getType())) {
                long s = start.longValue();
                long e = end.longValue();
                //lucene range queries are inclusive, so adjust ranges as needed
                if (!startInclusive) {
                    s = Math.addExact(s, 1);
                }
                if (!endInclusive) {
                    e = Math.addExact(e, -1);
                }

                return LongPoint.newRangeQuery(field, s, e);
            } else if (Double.class.equals(cfg.getType())) {
                double s = start.longValue();
                double e = end.longValue();
                //lucene range queries are inclusive, so adjust ranges as needed
                if (!startInclusive) {
                    s = Math.nextAfter(s, Double.MIN_VALUE);
                }
                if (!endInclusive) {
                    e = Math.nextAfter(e, -Double.MIN_VALUE);
                }

                return DoublePoint.newRangeQuery(field, s, e);
            } else if (Float.class.equals(cfg.getType())) {
                float s = start.longValue();
                float e = end.longValue();
                //lucene range queries are inclusive, so adjust ranges as needed
                if (!startInclusive) {
                    s = Math.nextAfter(s, Float.MIN_VALUE);
                }
                if (!endInclusive) {
                    e = Math.nextAfter(e, -Float.MIN_VALUE);
                }

                return FloatPoint.newRangeQuery(field, s, e);
            } else {
                throw new ParseException("Unknown numeric type: " + cfg.getType().getCanonicalName());
            }
        }
    }

    public void setPointsConfig(@Nonnull Map<String, PointsConfig> pointsConfig) {
        this.pointsConfig = pointsConfig;
    }

    private Query addSlop(Query q, int slop) {
        if (q instanceof PhraseQuery) {
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            builder.setSlop(slop);
            PhraseQuery pq = (PhraseQuery)q;
            org.apache.lucene.index.Term[] terms = pq.getTerms();
            int[] positions = pq.getPositions();
            for (int i = 0; i < terms.length; ++i) {
                builder.add(terms[i], positions[i]);
            }
            q = builder.build();
        } else if (q instanceof MultiPhraseQuery) {
            MultiPhraseQuery mpq = (MultiPhraseQuery)q;

            if (slop != mpq.getSlop()) {
                q = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
            }
        } else if (q instanceof SpanNearQuery) {
            SpanNearQuery snq = (SpanNearQuery)q;
            if (slop != snq.getSlop()) {
                SpanNearQuery.Builder builder = new SpanNearQuery.Builder(snq.getField(), snq.isInOrder());
                for (SpanQuery sq : snq.getClauses()) {
                    builder.addClause(sq);
                }
                builder.setSlop(slop);
                q = builder.build();
            }
        }
        return q;
    }
}

/*
 * LuceneOptimizedParser.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.lucene.query.BitSetQuery;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

import javax.annotation.Nonnull;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;

/**
 * a mixin interface for common functionality of parsers, it provides the ability to
 * construct queries having typing information in hand via {@link PointsConfig}.
 */
public interface ConfigAwareQueryParser {

    @Nonnull
    Map<String, PointsConfig> getPointsConfig();

    @Nonnull
    Query constructFieldWithoutPointsConfig(String field, String queryText, boolean quoted) throws ParseException;

    @Nonnull
    Token nextToken();

    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    @Nonnull
    default Query attemptConstructFieldQueryWithPointsConfig(final String field, String queryText, final boolean quoted) throws ParseException {
        final var pointsConfig = getPointsConfig();
        PointsConfig cfg = pointsConfig.get(field);
        if (cfg == null) {
            return constructFieldWithoutPointsConfig(field, queryText, quoted);
        }

        /*
         * Look for BITSET_CONTAINS function. If it is there, read the mask to get the number,
         * and then create a BITSET query operator.
         */
        if ("BITSET_CONTAINS".equalsIgnoreCase(queryText)) {
            return constructBitSetQuery(field);
        }

        //parse the text as the correct type and convert it to a query

        if (cfg instanceof BooleanPointsConfig) {
            //this is a boolean field, so change it to a binary type

            if ("true".equalsIgnoreCase(queryText)) {
                return BinaryPoint.newExactQuery(field, BooleanPointsConfig.TRUE_BYTES);
            } else {
                return BinaryPoint.newExactQuery(field, BooleanPointsConfig.FALSE_BYTES);
            }
        }

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

    @Nonnull
    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    private Query constructBitSetQuery(@Nonnull final String field) throws ParseException {
        //look for the next token
        if (!"(".equals(nextToken().toString())) {
            throw new ParseException("Missing ( from BITSET_CONTAINS");
        }
        Token nextToken = nextToken(); //this should be the actual value
        String bitMaskStr = nextToken.toString();
        //check for a ), in order to consume it. If it's not there, throw a Parse Exception
        if (!")".equals(nextToken().toString())) {
            throw new ParseException("Missing ) from BITSET_CONTAINS");
        }

        final var cfg = Objects.requireNonNull(getPointsConfig()).get(field);
        if (!Long.class.equals(cfg.getType())) {
            throw new ParseException("Cannot parse a BITSET_CONTAINS on a non-long data type");
        }

        long bitMask = -1;
        try {
            bitMask = Long.parseLong(bitMaskStr);
        } catch (NumberFormatException pe) {
            throw new ParseException(QueryParserMessages.COULD_NOT_PARSE_NUMBER);
        }

        return new BitSetQuery(field, bitMask);
    }

    @Nonnull
    Query constructRangeQueryWithoutPointsConfig(String field,
                                                 String part1,
                                                 String part2,
                                                 boolean startInclusive,
                                                 boolean endInclusive) throws ParseException;

    @SuppressWarnings("PMD.PreserveStackTrace") //it isn't possible with Lucene's exception API
    @Nonnull
    default Query attemptConstructRangeQueryWithPointsConfig(final String field,
                                                             final String part1,
                                                             final String part2,
                                                             final boolean startInclusive,
                                                             final boolean endInclusive) throws ParseException {
        final var pointsConfig = getPointsConfig();
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
            return constructRangeQueryWithoutPointsConfig(field, part1, part2, startInclusive, endInclusive);
        } else if (cfg instanceof BooleanPointsConfig) {
            byte[] p1 = "true".equalsIgnoreCase(part1) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
            byte[] p2 = "true".equalsIgnoreCase(part2) ? BooleanPointsConfig.TRUE_BYTES : BooleanPointsConfig.FALSE_BYTES;
            return BinaryPoint.newRangeQuery(field, p1, p2);
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
                return newIntegerRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Long.class.equals(cfg.getType())) {
                return newLongRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Double.class.equals(cfg.getType())) {
                return newDoubleRangeQuery(field, startInclusive, endInclusive, start, end);
            } else if (Float.class.equals(cfg.getType())) {
                return newFloatRangeQuery(field, startInclusive, endInclusive, start, end);
            } else {
                throw new ParseException(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE);
            }
        }
    }

    @SpotBugsSuppressWarnings(value = "FE_FLOATING_POINT_EQUALITY", justification = "Floating point values are special sentinel values")
    @Nonnull
    private Query newFloatRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        float s = start.floatValue();
        float e = end.floatValue();
        if (s > e) {
            //probably not the best error message, but it's what Lucene offers us
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }

        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Float.MAX_VALUE || s == Float.POSITIVE_INFINITY) {
                return FloatPoint.newSetQuery(field);
            } else {
                s = Math.nextAfter(s, Float.MAX_VALUE);
            }
        }
        if (!endInclusive) {
            if (e == Float.MIN_VALUE || e == Float.NEGATIVE_INFINITY) {
                return FloatPoint.newSetQuery(field);
            } else {
                e = Math.nextAfter(e, -Float.MAX_VALUE);
            }
        }

        return FloatPoint.newRangeQuery(field, s, e);
    }

    @SpotBugsSuppressWarnings(value = "FE_FLOATING_POINT_EQUALITY", justification = "Floating point values are special sentinel values")
    @Nonnull
    private Query newDoubleRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        double s = start.doubleValue();
        double e = end.doubleValue();

        if (s > e) {
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Double.MAX_VALUE || s == Double.POSITIVE_INFINITY) {
                return DoublePoint.newSetQuery(field);
            } else {
                s = Math.nextAfter(s, Double.MAX_VALUE);
            }
        }
        if (!endInclusive) {
            if (e == Double.MIN_VALUE || e == Double.NEGATIVE_INFINITY) {
                return DoublePoint.newSetQuery(field);
            } else {
                e = Math.nextAfter(e, -Double.MAX_VALUE);
            }
        }

        return DoublePoint.newRangeQuery(field, s, e);
    }

    @Nonnull
    private Query newLongRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        long s = start.longValue();
        long e = end.longValue();
        if (s > e) {
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        /*
         * we need to adjust ranges to remove inclusive values if we need to.
         *
         * If s == Long.MAX_VALUE, then we can't increment it without potentially
         * causing an error (due to long overflows), but we know that if you are specifying
         * the range as (MAX_VALUE,...) then that is an empty set by definition, so
         * we  return a Query that will always be empty. Similarly if we have e == Long.MIN_VALUE
         * and we want to be exclusive on the end point
         */
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Long.MAX_VALUE) {
                //does a point-in-set query but with an empty set, so should always return false.
                //there may be cheaper ways to do this in Lucene, but I'm not aware of them
                return LongPoint.newSetQuery(field);
            } else {
                s = Math.incrementExact(s);
            }
        }
        if (!endInclusive) {
            if (e == Long.MIN_VALUE) {
                return LongPoint.newSetQuery(field);
            } else {
                e = Math.decrementExact(e);
            }
        }

        return LongPoint.newRangeQuery(field, s, e);
    }

    @Nonnull
    private Query newIntegerRangeQuery(final String field, final boolean startInclusive, final boolean endInclusive, final Number start, final Number end) throws ParseException {
        int s = start.intValue();
        int e = end.intValue();
        if (s > e) {
            //probably not the best error message, but it's what Lucene offers us
            throw new ParseException(QueryParserMessages.INVALID_SYNTAX);
        }
        //lucene range queries are inclusive, so adjust ranges as needed
        if (!startInclusive) {
            if (s == Integer.MAX_VALUE) {
                return IntPoint.newSetQuery(field);
            } else {
                s = Math.addExact(s, 1);
            }
        }
        if (!endInclusive) {
            if (e == Integer.MIN_VALUE) {
                return IntPoint.newSetQuery(field);
            } else {
                e = Math.addExact(e, -1);
            }
        }

        return IntPoint.newRangeQuery(field, s, e);
    }

    @Nonnull
    default Query addSlop(Query q, int slop) {
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

/*
 * LuceneQueryClause.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.query.BitSetQuery;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Binder for a single query clause.
 */
@API(API.Status.UNSTABLE)
public abstract class LuceneQueryClause implements PlanHashable {
    @Nonnull
    private final LuceneQueryType queryType;

    protected LuceneQueryClause(@Nonnull final LuceneQueryType queryType) {
        this.queryType = queryType;
    }

    @Nonnull
    public LuceneQueryType getQueryType() {
        return queryType;
    }

    public abstract BoundQuery bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context);

    public abstract void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder);

    @Nonnull
    protected BoundQuery toBoundQuery(@Nonnull final Query luceneQuery) {
        return BoundQuery.ofLuceneQueryWithQueryType(luceneQuery, getQueryType());
    }

    /**
     * Parse the Lucene query to get all the mapping from field to terms.
     * @param query lucene query to extract all terms from
     * @return a new highlighting terms map
     */
    @Nonnull
    protected static Map<String, Set<String>> getHighlightingTermsMap(@Nonnull final Query query) {
        Map<String, Set<String>> highlightingTermsMap = Maps.newHashMap();
        if (query instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) query;
            for (BooleanClause clause : booleanQuery.clauses()) {
                combineHighlightingTermsMaps(highlightingTermsMap, getHighlightingTermsMap(clause.getQuery()));
            }
        } else if (query instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) query;
            Term term = termQuery.getTerm();
            highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
            highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT));
        } else if (query instanceof PhraseQuery) {
            PhraseQuery phraseQuery = (PhraseQuery) query;
            for (Term term : phraseQuery.getTerms()) {
                highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
                highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT));
            }
        } else if (query instanceof MultiPhraseQuery) {
            MultiPhraseQuery multiPhraseQuery = (MultiPhraseQuery) query;
            for (Term[] termArray : multiPhraseQuery.getTermArrays()) {
                for (Term term : termArray) {
                    highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
                    highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT));
                }
            }
        } else if (query instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) query;
            combineHighlightingTermsMaps(highlightingTermsMap, getHighlightingTermsMap(boostQuery.getQuery()));
        } else if (query instanceof SynonymQuery) {
            SynonymQuery synonymQuery = (SynonymQuery)query;
            for (Term term : synonymQuery.getTerms()) {
                highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
                highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT));
            }
        } else if (query instanceof SpanOrQuery) {
            SpanOrQuery spanOrQuery = (SpanOrQuery)query;
            for (SpanQuery clause : spanOrQuery.getClauses()) {
                combineHighlightingTermsMaps(highlightingTermsMap, getHighlightingTermsMap(clause));
            }
        } else if (query instanceof SpanNearQuery) {
            SpanNearQuery spanNearQuery = (SpanNearQuery)query;
            for (SpanQuery clause : spanNearQuery.getClauses()) {
                combineHighlightingTermsMaps(highlightingTermsMap, getHighlightingTermsMap(clause));
            }
        } else if (query instanceof SpanTermQuery) {
            SpanTermQuery spanTermQuery = (SpanTermQuery)query;
            Term term = spanTermQuery.getTerm();
            highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
            highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT));
        } else if (query instanceof PrefixQuery) {
            PrefixQuery termQuery = (PrefixQuery) query;
            Term term = termQuery.getPrefix();
            highlightingTermsMap.putIfAbsent(term.field(), new HashSet<>());
            highlightingTermsMap.get(term.field()).add(term.text().toLowerCase(Locale.ROOT) + "*");
        } else if (query instanceof BitSetQuery) {
            BitSetQuery bitsetQuery = (BitSetQuery)query;
            String field = bitsetQuery.getField();
            highlightingTermsMap.computeIfAbsent(field, key -> new HashSet<>()).add(field.toLowerCase(Locale.ROOT));
        } else if (query instanceof PointRangeQuery) {
            PointRangeQuery pointRangeQuery = (PointRangeQuery)query;
            String field = pointRangeQuery.getField();
            highlightingTermsMap.computeIfAbsent(field, key -> new HashSet<>()).add(field.toLowerCase(Locale.ROOT));
        } else {
            throw new RecordCoreException("This lucene query is not supported for highlighting");
        }
        return highlightingTermsMap;
    }

    @CanIgnoreReturnValue
    @Nonnull
    protected static Map<String, Set<String>> combineHighlightingTermsMaps(@Nonnull final Map<String, Set<String>> existingMap,
                                                                           @Nonnull final Map<String, Set<String>> newMap) {
        newMap.forEach((field, newTerms) ->
                existingMap.merge(field, newTerms, (o, n) -> {
                    final Set<String> terms = Sets.newHashSet(o);
                    terms.addAll(n);
                    return terms;
                }));
        return existingMap;
    }

    /**
     * Helper class to capture a bound query, i.e. a lucene query where all parameters have been resolved.
     */
    public static class BoundQuery {
        @Nonnull
        private final Query luceneQuery;
        @Nullable
        private final Map<String, Set<String>> highlightingTermsMap;

        public BoundQuery(@Nonnull final Query luceneQuery) {
            this(luceneQuery, null);
        }

        public BoundQuery(@Nonnull final Query luceneQuery, @Nullable final Map<String, Set<String>> highlightingTermsMap) {
            this.luceneQuery = luceneQuery;
            this.highlightingTermsMap = highlightingTermsMap;
        }

        @Nonnull
        public Query getLuceneQuery() {
            return luceneQuery;
        }

        @Nullable
        public Map<String, Set<String>> getHighlightingTermsMap() {
            return highlightingTermsMap;
        }

        public static BoundQuery ofLuceneQueryWithQueryType(@Nonnull Query luceneQuery, @Nonnull LuceneQueryType queryType) {
            if (queryType == LuceneQueryType.QUERY_HIGHLIGHT) {
                return new BoundQuery(luceneQuery, LuceneQueryClause.getHighlightingTermsMap(luceneQuery));
            }
            return new BoundQuery(luceneQuery);
        }
    }
}

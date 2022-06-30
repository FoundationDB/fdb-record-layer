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
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * Optimized {@link QueryParser} that adds the slop for {@link SpanNearQuery} as well.
 * So the proximity search based on {@link SpanNearQuery} can also work.
 */
public class LuceneOptimizedQueryParser extends QueryParser {

    public LuceneOptimizedQueryParser(String field, Analyzer analyzer) {
        super(field, analyzer);
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop)
            throws ParseException {
        Query query = getFieldQuery(field, queryText, true);

        if (query instanceof PhraseQuery) {
            query = addSlopToPhrase((PhraseQuery) query, slop);
        } else if (query instanceof MultiPhraseQuery) {
            MultiPhraseQuery mpq = (MultiPhraseQuery)query;

            if (slop != mpq.getSlop()) {
                query = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
            }
        } else if (query instanceof SpanNearQuery) {
            SpanNearQuery snq = (SpanNearQuery) query;
            if (slop != snq.getSlop()) {
                SpanNearQuery.Builder builder = new SpanNearQuery.Builder(snq.getField(), snq.isInOrder());
                for (SpanQuery sq : snq.getClauses()) {
                    builder.addClause(sq);
                }
                builder.setSlop(slop);
                query = builder.build();
            }
        }

        return query;
    }

    private PhraseQuery addSlopToPhrase(PhraseQuery query, int slop) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        org.apache.lucene.index.Term[] terms = query.getTerms();
        int[] positions = query.getPositions();
        for (int i = 0; i < terms.length; ++i) {
            builder.add(terms[i], positions[i]);
        }

        return builder.build();
    }
}

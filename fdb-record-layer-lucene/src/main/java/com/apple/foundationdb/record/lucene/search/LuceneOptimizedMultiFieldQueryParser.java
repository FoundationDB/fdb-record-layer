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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimized {@link MultiFieldQueryParser} that adds the slop for {@link SpanNearQuery} as well.
 * So the proximity search based on {@link SpanNearQuery} can also work.
 */
public class LuceneOptimizedMultiFieldQueryParser extends MultiFieldQueryParser {

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
                    q = applySlop(q,slop);
                    clauses.add(q);
                }
            }
            if (clauses.isEmpty()) {  // happens for stopwords
                return null;
            }
            return getMultiFieldQuery(clauses);
        }
        Query q = super.getFieldQuery(field, queryText, true);
        q = applySlop(q,slop);
        return q;
    }

    private Query applySlop(Query q, int slop) {
        if (q instanceof PhraseQuery) {
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            builder.setSlop(slop);
            PhraseQuery pq = (PhraseQuery) q;
            org.apache.lucene.index.Term[] terms = pq.getTerms();
            int[] positions = pq.getPositions();
            for (int i = 0; i < terms.length; ++i) {
                builder.add(terms[i], positions[i]);
            }
            q = builder.build();
        } else if (q instanceof MultiPhraseQuery) {
            MultiPhraseQuery mpq = (MultiPhraseQuery) q;

            if (slop != mpq.getSlop()) {
                q = new MultiPhraseQuery.Builder(mpq).setSlop(slop).build();
            }
        } else if (q instanceof SpanNearQuery) {
            SpanNearQuery snq = (SpanNearQuery) q;
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

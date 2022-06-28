/*
 * MailboxWeight.java
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

package com.apple.foundationdb.record.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

public class CustomWeight extends Weight {
    private final Function<LeafReaderContext, DocIdSetIterator> fxn;
    /**
     * Sole constructor, typically invoked by sub-classes.
     *
     * @param query the parent query
     */
    protected CustomWeight(final Query query, Function<LeafReaderContext, DocIdSetIterator> fxn) {
        super(query);
        this.fxn = fxn;
    }

    @Override
    public ScorerSupplier scorerSupplier(final LeafReaderContext context) throws IOException {
        return super.scorerSupplier(context);
    }

    @Override
    public BulkScorer bulkScorer(final LeafReaderContext context) throws IOException {
        return super.bulkScorer(context);
    }


    @Override
    @Deprecated
    public void extractTerms(final Set<Term> terms) {
        // No-op
    }

    @Override
    public Matches matches(final LeafReaderContext context, final int doc) throws IOException {
        return null;
    }

    @Override
    public Explanation explain(final LeafReaderContext context, final int doc) throws IOException {
        return null;
    }

    @Override
    public Scorer scorer(final LeafReaderContext context) throws IOException {
        return new CustomScorer(this, context, fxn);
    }

    @Override
    public boolean isCacheable(final LeafReaderContext ctx) {
        return false;
    }
}

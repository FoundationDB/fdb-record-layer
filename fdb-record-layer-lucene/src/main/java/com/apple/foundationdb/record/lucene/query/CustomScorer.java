/*
 * MailboxScorer.java
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.function.Function;

public class CustomScorer extends Scorer {
    private final LeafReaderContext context;

    private final Function<LeafReaderContext, DocIdSetIterator> fxn;
    private DocIdSetIterator iterator;

    /**
     * Constructs a Scorer
     *
     * @param weight The scorers <code>Weight</code>.
     */
    protected CustomScorer(final Weight weight, final LeafReaderContext context, final Function<LeafReaderContext, DocIdSetIterator> fxn) {
        super(weight);
        this.context = context;
        this.fxn = fxn;
        this.iterator = fxn.apply(context);
    }

    @Override
    public DocIdSetIterator iterator() {
        return iterator;
    }

    @Override
    public float getMaxScore(final int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        return 0;
    }

    @Override
    public int docID() {
        return iterator.docID();
    }
}

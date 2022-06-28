/*
 * MailboxQuery.java
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import java.io.IOException;
import java.util.function.Function;


/**
 * Class that takes in a fxn that accepts the leafReaderContext and returns
 * an ordered DocIdSetIterator.  The fxn can choose to filter out possible values
 * with this iterator.
 *
 * So for a generic example, you could allow all values by providing a function like:
 *
 *  (leafReaderContext -> DocIdSetIterator.all(Integer.MAX_VALUE)
 *
 */
public class CustomQuery extends Query {
    private final Function<LeafReaderContext, DocIdSetIterator> fxn;
    public CustomQuery(Function<LeafReaderContext, DocIdSetIterator> fxn) {
        this.fxn = fxn;
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher, final ScoreMode scoreMode, final float boost) throws IOException {
        return new CustomWeight(this, fxn);
    }

    @Override
    public String toString(final String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CustomQuery {");
        if (field != null) {
            buffer.append("field=");
            buffer.append(field);
        }
        buffer.append("}");
        return buffer.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        return this.equals(obj);
    }

    @Override
    public int hashCode() {
        return fxn.hashCode();
    }

}

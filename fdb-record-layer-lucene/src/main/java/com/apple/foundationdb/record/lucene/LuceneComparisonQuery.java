/*
 * LuceneComparisonQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * Wrapper of a Lucene {@link Query} that contains accessible
 * field name, comparison type, and comparand.
 */
public class LuceneComparisonQuery extends Query {
    private final Query query;
    private final String fieldName;
    private final Comparisons.Type comparisonType;
    private final Object comparand;

    public LuceneComparisonQuery(@Nonnull final Query query,
                                 @Nonnull final String fieldName,
                                 @Nonnull final Comparisons.Type comparisonType,
                                 @Nullable final Object comparand) {
        this.query = query;
        this.fieldName = fieldName;
        this.comparisonType = comparisonType;
        this.comparand = comparand;
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher, final ScoreMode scoreMode, final float boost) throws IOException {
        return query.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(final IndexReader reader) throws IOException {
        return query.rewrite(reader);
    }

    @Override
    public void visit(final QueryVisitor visitor) {
        query.visit(visitor);
    }

    public String getFieldName() {
        return fieldName;
    }

    public Comparisons.Type getComparisonType() {
        return comparisonType;
    }

    public Object getComparand() {
        return comparand;
    }

    @Override
    public String toString(final String field) {
        return query.toString(field);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof LuceneComparisonQuery &&
                Objects.equals(fieldName, ((LuceneComparisonQuery)obj).fieldName) &&
                Objects.equals(comparisonType, ((LuceneComparisonQuery)obj).comparisonType) &&
                Objects.equals(comparand, ((LuceneComparisonQuery)obj).comparand) &&
                Objects.equals(query, ((LuceneComparisonQuery)obj).query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, comparisonType, comparand, query);
    }
}

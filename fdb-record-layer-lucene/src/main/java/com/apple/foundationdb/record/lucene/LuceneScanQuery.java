/*
 * LuceneScanQuery.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scan a {@code LUCENE} index using a Lucene {@link Query}.
 */
@API(API.Status.UNSTABLE)
public class LuceneScanQuery extends LuceneScanBounds {
    @Nonnull
    private final Query query;
    @Nullable
    private final Sort sort;
    @Nullable
    private final List<String> storedFields;
    @Nullable
    private final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes;

    @Nullable
    private final LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters;

    @Nullable
    private final Map<String, Set<String>> termMap;

    public LuceneScanQuery(@Nonnull final IndexScanType scanType, @Nonnull final Tuple groupKey,
                           @Nonnull final Query query, @Nullable final Sort sort, @Nullable final List<String> storedFields,
                           @Nullable final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes,
                           @Nullable final LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters,
                           @Nullable final Map<String, Set<String>> termMap) {
        super(scanType, groupKey);
        this.query = query;
        this.sort = sort;
        this.storedFields = storedFields;
        this.storedFieldTypes = storedFieldTypes;
        this.luceneQueryHighlightParameters = luceneQueryHighlightParameters;
        this.termMap = termMap;
    }

    @Nonnull
    public Query getQuery() {
        return query;
    }

    @Nullable
    public Sort getSort() {
        return sort;
    }

    @Nullable
    public List<String> getStoredFields() {
        return storedFields;
    }

    @Nullable
    public List<LuceneIndexExpressions.DocumentFieldType> getStoredFieldTypes() {
        return storedFieldTypes;
    }

    @Nullable
    public LuceneScanQueryParameters.LuceneQueryHighlightParameters getLuceneQueryHighlightParameters() {
        return luceneQueryHighlightParameters;
    }

    @Nullable
    public Map<String, Set<String>> getTermMap() {
        return termMap;
    }

    @Override
    public String toString() {
        return super.toString() + " " + query;
    }
}

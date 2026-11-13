/*
 * LuceneScanQuery.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

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

    private final boolean includeScoreValue;

    public LuceneScanQuery(@Nonnull final IndexScanType scanType, @Nonnull final Tuple groupKey,
                           @Nonnull final Query query, @Nullable final Sort sort, @Nullable final List<String> storedFields,
                           @Nullable final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes,
                           @Nullable final LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters,
                           @Nullable final Map<String, Set<String>> termMap) {
        this(scanType, groupKey, query, sort, storedFields, storedFieldTypes, luceneQueryHighlightParameters, termMap, false);
    }

    /**
     * Create a scan of a {@code LUCENE} index.
     *
     * @param includeScoreValue whether each returned {@link LuceneRecordCursor.ScoreDocIndexEntry} must expose its Lucene
     * score through {@link LuceneRecordCursor.ScoreDocIndexEntry#getScore()}. This is only supported when the results
     * are ranked by relevance, see {@link #isRelevanceSort(Sort)}
     * @throws RecordCoreArgumentException if {@code includeScoreValue} is set and the sort is not by relevance
     */
    public LuceneScanQuery(@Nonnull final IndexScanType scanType, @Nonnull final Tuple groupKey,
                           @Nonnull final Query query, @Nullable final Sort sort, @Nullable final List<String> storedFields,
                           @Nullable final List<LuceneIndexExpressions.DocumentFieldType> storedFieldTypes,
                           @Nullable final LuceneScanQueryParameters.LuceneQueryHighlightParameters luceneQueryHighlightParameters,
                           @Nullable final Map<String, Set<String>> termMap,
                           final boolean includeScoreValue) {
        super(scanType, groupKey);
        validateScoreValueSupported(sort, includeScoreValue);
        this.query = query;
        this.sort = sort;
        this.storedFields = storedFields;
        this.storedFieldTypes = storedFieldTypes;
        this.luceneQueryHighlightParameters = luceneQueryHighlightParameters;
        this.termMap = termMap;
        this.includeScoreValue = includeScoreValue;
    }

    /**
     * Whether the results of a search with the given sort are ranked by (descending) relevance. That is the case for an
     * unsorted search, and for one whose primary sort field is the score. Only then is the Lucene score of each hit
     * well defined for the caller.
     *
     * @param sort the sort of the search, if any
     * @return {@code true} if the primary ordering is by descending score
     */
    public static boolean isRelevanceSort(@Nullable final Sort sort) {
        if (sort == null) {
            return true;
        }
        final SortField[] sortFields = sort.getSort();
        return sortFields.length > 0 && sortFields[0].getType() == SortField.Type.SCORE && !sortFields[0].getReverse();
    }

    /**
     * Check that the score can be reported for a search with the given sort.
     *
     * @param sort the sort of the search, if any
     * @param includeScoreValue whether the score is requested
     * @throws RecordCoreArgumentException if the score is requested and the sort is not by relevance
     */
    static void validateScoreValueSupported(@Nullable final Sort sort, final boolean includeScoreValue) {
        if (includeScoreValue && !isRelevanceSort(sort)) {
            throw new RecordCoreArgumentException("Unsupported configuration: the score can only be included when results are sorted by relevance",
                    "sort", sort);
        }
    }

    public boolean isIncludeScoreValue() {
        return includeScoreValue;
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

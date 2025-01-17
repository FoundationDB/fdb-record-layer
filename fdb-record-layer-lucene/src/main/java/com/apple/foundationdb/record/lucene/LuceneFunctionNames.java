/*
 * LuceneFunctionNames.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

/**
 * Key function names for Lucene indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneFunctionNames {
    public static final String LUCENE_FIELD_NAME = "lucene_field_name";
    public static final String LUCENE_SORTED = "lucene_sorted";
    public static final String LUCENE_STORED = "lucene_stored";
    public static final String LUCENE_TEXT = "lucene_text";
    public static final String LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS = "lucene_full_text_field_index_options";
    public static final String LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTORS = "lucene_full_text_field_with_term_vectors";
    public static final String LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTOR_POSITIONS = "lucene_full_text_field_with_term_vector_positions";
    public static final String LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS = "lucene_auto_complete_field_index_options";
    public static final String LUCENE_SORT_BY_RELEVANCE = "lucene_sort_by_relevance";
    public static final String LUCENE_SORT_BY_DOCUMENT_NUMBER = "lucene_sort_by_document_number";

    public static final String LUCENE_HIGHLIGHT_TAG = "lucene_highlight_tag";

    private LuceneFunctionNames() {
    }

    /**
     * Option keys for {@link #LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS}.
     */
    public enum LuceneFieldIndexOptions {
        DOCS,
        DOCS_AND_FREQS,
        DOCS_AND_FREQS_AND_POSITIONS,
        DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
    }
}

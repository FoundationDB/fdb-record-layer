/*
 * LuceneOptimizedMultiFieldStopWordsQueryParser.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A {@link QueryParser} that changes the way by which stop words in the query are handled.
 * Stop words are not indexed, which means that a query for {@code +the} will not match a document that has "the" in it
 * (because of the "+" in the query and the fact that the "the" is not indexed).
 * In the cases where users are adding search terms that are prefixes of stop words (e.g. {@code +the*}) we want to match
 * documents that have suffixes of the stop words (e.g. "these") but not be blocked by the stop words themselves.
 * As a result, this parser relaxes the query requirement (removed the "+") from stop words search terms.
 * Note that normally, for search terms that are not prefix queries (ones that do not end with "*"), the analyzer will handle
 * the stop words removal, so this is not necessary here.
 */
@SuppressWarnings({"java:S110"})
public class LuceneOptimizedMultiFieldStopWordsQueryParser extends LuceneOptimizedMultiFieldQueryParser {
    @Nonnull
    private final CharArraySet stopWords;

    public LuceneOptimizedMultiFieldStopWordsQueryParser(final String[] fields, final Analyzer analyzer, @Nonnull final Map<String, PointsConfig> pointsConfig, @Nonnull CharArraySet stopWords) {
        super(fields, analyzer, pointsConfig);
        this.stopWords = stopWords;
    }

    @Override
    protected BooleanClause newBooleanClause(final Query q, final BooleanClause.Occur occur) {
        return super.newBooleanClause(q, QueryParserUtils.relaxOccur(q, occur, stopWords));
    }
}

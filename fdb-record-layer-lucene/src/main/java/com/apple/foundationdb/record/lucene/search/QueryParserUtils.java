/*
 * QueryParserUtils.java
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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class QueryParserUtils {
    /**
     * Relax the modifier for the query, if possible.
     * When there is a "MUST" for a prefix query on a stop word, turn that into a "SHOULD"
     * @param q the query
     * @param occur the query modifier
     * @param stopWords the list of stop words
     * @return the modifier, modified if it should be relaxed
     */
    @Nonnull
    public static BooleanClause.Occur relaxOccur(@Nonnull final Query q, @Nonnull final BooleanClause.Occur occur, @Nonnull final CharArraySet stopWords) {
        BooleanClause.Occur modifiableOccur = occur;

        CharSequence term = getTerm(q);
        if ((term != null) && (stopWords.contains(term))) {
            // Prefix queries with "+" ("+term*") are relaxed to become "term*"
            modifiableOccur = BooleanClause.Occur.SHOULD;
        }
        return modifiableOccur;
    }

    /**
     * Return the search term for the query.
     * This will return the search term for the query, if the query is one that should go through stop word handling.
     * For now, only prefix queries ("term*") are being handled (non-prefix queries have stop words handled in the analyzer).
     *
     * @param q the query
     * @return the search term if the query is "of interest", null otherwise
     */
    @Nullable
    private static CharSequence getTerm(final Query q) {
        if (q instanceof PrefixQuery) {
            return ((PrefixQuery)q).getPrefix().text();
        } else {
            return null;
        }
    }
}

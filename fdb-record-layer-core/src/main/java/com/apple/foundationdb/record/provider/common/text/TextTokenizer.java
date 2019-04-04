/*
 * TextTokenizer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common.text;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An interface to tokenize text fields. Implementations of this interface should handle normalization,
 * stemming, case-folding, stop-word removal, and all other analysis steps to generate a token list
 * from raw text. When indexing, the {@link TextIndexMaintainer} will use an instance of this class
 * to generate the token list from text and use that to generate the position list for each token.
 * Each implementation should also implement a {@link TextTokenizerFactory} that instantiates instances of
 * that class. The factory class can then be picked up by a {@link TextTokenizerRegistry}
 * and used by the {@link TextIndexMaintainer} to tokenize text while indexing.
 *
 * <p>
 * To correctly maintain indexes, it is important that each tokenizer be deterministic for a
 * given input, and that the tokenizing logic be frozen once data are written to the database with
 * that tokenizer. To support backwards-incompatible tokenizer updates (for example, adding or removing
 * stop words or making different normalization decisions), one can create a new "version" of that
 * tokenizer. The version number is passed to the tokenizer at tokenize-time through the
 * <code>version</code> parameter. One should continue to support using older versions until
 * such time as all data that were written with the old version have been migrated to the
 * new version. At that time, one can drop support for the older version by increasing the value
 * returned by {@link #getMinVersion()} to exclude the older tokenizers.
 * </p>
 *
 * @see TextTokenizerRegistry
 * @see com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("squid:S1214") // constants in interfaces
public interface TextTokenizer {
    /**
     * The absolute minimum tokenizer version. All tokenizers should begin at
     * this version and work their way up. If no explicit tokenizer version is
     * included in the meta-data, the index maintainer will use this version.
     */
    int GLOBAL_MIN_VERSION = 0;

    /**
     * Mode that can be used to alter tokenizer behavior depending on the
     * the context its used in. In particular, some tokenizers (such as
     * an n-gram tokenizer) might tokenize a document differently at index
     * time than how they tokenize a query string at query time. To support
     * this, implementations may choose to alter behavior based on the value
     * of this flag.
     */
    enum TokenizerMode {
        /**
         * Tokenize a {@link String} as if it were about to be indexed. This should
         * generally be the mode used when tokenizing documents.
         */
        INDEX,
        /**
         * Tokenize a {@link String} as if it were about to be used as the arguments
         * to a query. This should generally be the mode used when tokenizing query
         * strings.
         */
        QUERY
    }

    /**
     * Create a stream of tokens from the given input text. This should encapsulate all analysis
     * done on the text to produce a sensible token list, i.e., the user should not assume that
     * any normalization or text processing is done on the results from this function. To indicate
     * the presence of an un-indexed token (like a stop word), this should emit the empty string
     * in its place (so that the position list is correct). The version parameter of this method
     * can be used to maintain old behavior when necessary as the tokenizer is updated.
     *
     * @param text source text to tokenize
     * @param version version of the tokenizer to use
     * @param mode whether this tokenizer is being used to index a document or query a set of documents
     * @return a stream of tokens retrieved from the text
     */
    @Nonnull
    Iterator<? extends CharSequence> tokenize(@Nonnull String text, int version, @Nonnull TokenizerMode mode);

    /**
     * Create a map from tokens to their offset lists from the given input text. This
     * should be consistent with the {@link #tokenize(String, int, TokenizerMode) tokenize()} function
     * in that it should apply the same analysis on the token list as that function
     * does (or call that function directly). By default, this calls <code>tokenize()</code>
     * to produce a token stream and then inserts each token into a map. It keeps track
     * of the current number of tokens and updates the value of the map with additional
     * offsets. More exotic implementations of this function could, for example, decide
     * to stop tokenizing the source text after reaching a maximum number of unique tokens
     * or write a different offset list than would be done by default. But if that
     * behavior changes, it is the responsibility of the tokenizer maintainer
     * to bump the version of this tokenizer so that the old behavior can be reliably
     * replicated at a future date.
     *
     * <p>
     * The {@link com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer TextIndexMaintainer}
     * will use this method to tokenize a document into a map and place each entry into
     * the database. This method is not used by queries (which instead use the
     * <code>tokenize</code> method instead).
     * </p>
     *
     * @param text source text to tokenize
     * @param version version of the tokenizer to use
     * @param mode whether this tokenizer is being used to index a document or query a set of documents
     * @return a mapping from token to a list of offsets in the original text
     */
    @Nonnull
    default Map<String, List<Integer>> tokenizeToMap(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        final Iterator<? extends CharSequence> tokens = tokenize(text, version, mode);
        Map<String, List<Integer>> offsetMap = new HashMap<>();
        int offset = 0;
        while (tokens.hasNext()) {
            final String token = tokens.next().toString();
            if (!token.isEmpty()) {
                offsetMap.computeIfAbsent(token, ignore -> new ArrayList<>()).add(offset);
            }
            offset += 1;
        }
        return offsetMap;
    }

    /**
     * Create a list of tokens from the given input text. By default, this will just
     * run the {@link #tokenize(String, int, TokenizerMode) tokenize()} method on the given text at
     * the given version and then add all of the elements to a list.
     *
     * @param text source text to tokenize
     * @param version version of the tokenizer to use
     * @param mode whether this tokenizer is being used to index a document or query a set of documents
     * @return a list of tokens retrieved from the text
     */
    default List<String> tokenizeToList(@Nonnull String text, int version, @Nonnull TokenizerMode mode) {
        final Iterator<? extends CharSequence> tokens = tokenize(text, version, mode);
        List<String> tokenList = new ArrayList<>();
        while (tokens.hasNext()) {
            tokenList.add(tokens.next().toString());
        }
        return tokenList;
    }

    /**
     * The minimum supported version of this tokenizer. By default, this is the
     * {@link #GLOBAL_MIN_VERSION global minimum version}, which indicates that this
     * tokenizer can tokenize strings at all versions that this tokenizer type
     * has ever been able to tokenize. However, if this tokenizer has dropped support
     * for some older format, then this function should be implemented to
     *
     * @return the minimum supported tokenizer version
     */
    default int getMinVersion() {
        return GLOBAL_MIN_VERSION;
    }

    /**
     * The maximum supported version of this tokenizer. This should be greater
     * than or equal to the minimum version.
     *
     * @return the maximum supported tokenizer version
     */
    int getMaxVersion();

    /**
     * Get the name of this tokenizer. This should be the same name as is
     * returned by the corresponding {@link TextTokenizerFactory}'s
     * {@link TextTokenizerFactory#getName() getName()} method.
     *
     * @return this tokenizer's name
     */
    @Nonnull
    String getName();

    /**
     * Verify that the provided version is supported by this tokenizer. This
     * makes sure that the version given is greater than or equal to the
     * minimum version and less than or equal to the maximum version.
     *
     * @param version tokenizer version to verify is in bounds
     * @throws RecordCoreArgumentException if the version is out of bounds
     */
    default void validateVersion(int version) {
        if (version < getMinVersion() || version > getMaxVersion()) {
            throw new MetaDataException("unknown tokenizer version")
                    .addLogInfo(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, getName())
                    .addLogInfo(IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, version)
                    .addLogInfo("minVersion", getMinVersion())
                    .addLogInfo("maxVersion", getMaxVersion());
        }
    }
}

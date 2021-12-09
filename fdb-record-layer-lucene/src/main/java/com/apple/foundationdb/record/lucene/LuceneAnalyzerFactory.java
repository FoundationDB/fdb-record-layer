/*
 * LuceneAnalyzerFactory.java
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

import com.apple.foundationdb.record.metadata.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import javax.annotation.Nonnull;

/**
 * Each implementation of {@link Analyzer} should have its own implementation
 * of this factory interface to provide instances of the analyzers for indexing and query to a
 * {@link LuceneAnalyzerRegistry}. The registry will populate a mapping of names to
 * analyzers using the methods of this interface.
 *
 * @see LuceneIndexMaintainer
 */
public interface LuceneAnalyzerFactory {
    /**
     * Get the unique name for the text analyzer. This is the name that should
     * be included in the index meta-data in order to indicate that this analyzer
     * should be used within a certain index.
     *
     * @return the name of the analyzer that this factory creates
     */
    @Nonnull
    String getName();

    /**
     * Get an instance of the text analyzer for indexing given the {@link Index}. For a given factory, each indexing analyzer
     * should be of the same type, and it should match the result of {@link #getName()}.
     *
     * @param index the index this analyzer is used for
     * @return an instance of the analyzer for indexing that this factory creates
     */
    @Nonnull
    Analyzer getIndexAnalyzer(@Nonnull Index index);

    /**
     * Get an instance of the text analyzer for query given the {@link Index}. For a given factory, each query analyzer
     * should be of the same type, and it should match the result of {@link #getName()}.
     * Not need to override this method if {@link StandardAnalyzer} is to be used for query time.
     * Call {@link #getIndexAnalyzer(Index)} before calling this method, and use its return for the argument {@code indexAnalyzer}.
     * Override this method to customize the analyzer for query time, or directly return the {@code indexAnalyzer} instance, if it is used for both indexing and query time.
     *
     * @param index the index this analyzer is used for
     * @param indexAnalyzer the instance of analyzer for indexing used by this factory, that can be returned by this method in case it is also used for query
     * @return an instance of the analyzer for query that this factory creates, the default one is {@link StandardAnalyzer}
     */
    @Nonnull
    default Analyzer getQueryAnalyzer(@Nonnull Index index, @Nonnull Analyzer indexAnalyzer) {
        return new StandardAnalyzer();
    }
}

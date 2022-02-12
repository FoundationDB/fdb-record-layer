/*
 * LuceneAnalyzerRegistry.java
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;

import javax.annotation.Nonnull;

/**
 * Registry for {@link Analyzer}s. This registry allows for full-text indexes to specify
 * their analyzer through an index option, using the "{@value LuceneIndexOptions#TEXT_ANALYZER_NAME_OPTION}" option.
 * The registry will then be queried for the analyzer that has that name at query-time.
 *
 * <p>
 * Note that the way of adding elements to the analyzer registry is to use the
 * {@link com.google.auto.service.AutoService AutoService} annotation to mark a
 * {@link LuceneAnalyzerFactory} implementation as one that should be loaded into the registry.
 * </p>
 */
@SuppressWarnings("unused")
public interface LuceneAnalyzerRegistry {
    @Nonnull
    Pair<Analyzer, Analyzer> getLuceneAnalyzerPair(@Nonnull Index index);
}

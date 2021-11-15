/*
 * LuceneIndexMaintainerFactory.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Index Maintainer Factory for Lucene Indexes.  This adds the Lucene index to supported indexes.
 *
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexMaintainerFactory implements IndexMaintainerFactory {

    @Nonnull
    private static final List<String> TYPES = Collections.singletonList(LuceneIndexTypes.LUCENE);

    @Override
    public Iterable<String> getIndexTypes() {
        return TYPES;
    }

    @Nonnull
    @Override
    public IndexValidator getIndexValidator(@Nonnull Index index) {
        return new LuceneIndexValidator(index);
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        final Pair<Analyzer, Analyzer> analyzerPair = LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerPair(state.index);
        return new LuceneIndexMaintainer(state, state.context.getExecutor(), analyzerPair.getLeft(), analyzerPair.getRight());
    }
}

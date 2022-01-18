/*
 * LuceneSpellcheckResultCursor.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class LuceneSpellcheckResultCursor implements BaseCursor<IndexEntry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSpellcheckResultCursor.class);
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final IndexMaintainerState state;
    @Nullable
    private final FDBStoreTimer timer;
    private int limit;
    @Nullable
    private List<Lookup.LookupResult> lookupResults = null;
    private int currentPosition;
    @Nonnull
    private final String searchTerm;
    @Nullable
    private final String fieldName;
    private final AnalyzingInfixSuggester suggester;

    public LuceneSpellcheckResultCursor(final AnalyzingInfixSuggester suggester, final String value, final Executor executor, final ScanProperties scanProperties, final IndexMaintainerState state) {
        this.suggester = suggester;
        this.executor = executor;
        this.searchTerm = value;


    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<IndexEntry>> onNext() {
        return null;
    }

    @Override
    public void close() {

    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return null;
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        return false;
    }

    private void spellcheck() {

    }
}

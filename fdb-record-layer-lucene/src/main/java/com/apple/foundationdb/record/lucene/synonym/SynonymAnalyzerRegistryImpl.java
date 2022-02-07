/*
 * SynonymAnalyzer.java
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

package com.apple.foundationdb.record.lucene.synonym;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for {@link SynonymAnalyzer}s.
 */
public class SynonymAnalyzerRegistryImpl implements SynonymAnalyzerRegistry {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(SynonymAnalyzerRegistryImpl.class);

    private static final CharArraySet STOP_WORDS = EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

    private static final SynonymAnalyzerRegistryImpl INSTANCE = new SynonymAnalyzerRegistryImpl();

    @Nonnull
    private Map<String, SynonymAnalyzer> registry;

    private SynonymAnalyzerRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    public static SynonymAnalyzerRegistry instance() {
        return INSTANCE;
    }

    @Nonnull
    private static Map<String, SynonymAnalyzer> initRegistry() {
        final Map<String, SynonymAnalyzer> registry = new HashMap<>();
        registry.put(SynonymAnalyzer.DEFAULT_SYNONYM_SET,
                new SynonymAnalyzer(STOP_WORDS));
        return registry;
    }

    @Nonnull
    @Override
    public SynonymAnalyzer getAnalyzer(@Nonnull final String name) {
        final SynonymAnalyzer analyzer = registry.get(name);
        if (analyzer == null) {
            throw new MetaDataException("unrecognized synonym analyzer", LogMessageKeys.SYNONYM_NAME, name);
        }
        return analyzer;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public synchronized void register(@Nonnull final SynonymAnalyzer analyzer) {
        String name = analyzer.getName();
        SynonymAnalyzer oldAnalyzer = registry.putIfAbsent(name, analyzer);
        // If there was an already registered and the old analyzer isn't the same as
        // the new one, throw an error
        if (oldAnalyzer != null && oldAnalyzer != analyzer) {
            throw new RecordCoreArgumentException("attempted to register duplicate synonym analyzer", LogMessageKeys.SYNONYM_NAME, name);
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("registered tokenizer", LogMessageKeys.SYNONYM_NAME, name));
        }
    }

    @Override
    public void reset() {
        registry = initRegistry();
    }
}

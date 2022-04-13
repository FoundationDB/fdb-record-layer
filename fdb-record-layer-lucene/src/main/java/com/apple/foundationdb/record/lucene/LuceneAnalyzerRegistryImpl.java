/*
 * LuceneAnalyzerRegistryImpl.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Default implementation of the {@link LuceneAnalyzerRegistry}. It uses the class
 * loader to determine which {@link LuceneAnalyzerFactory} implementation exist,
 * and it populates the registry with those analyzers. An instance of this registry
 * is used by the {@link LuceneIndexMaintainer}
 * in order to choose the analyzer for a block of text.
 */
public class LuceneAnalyzerRegistryImpl implements LuceneAnalyzerRegistry {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneAnalyzerRegistryImpl.class);
    @Nonnull
    private static final LuceneAnalyzerRegistryImpl INSTANCE = new LuceneAnalyzerRegistryImpl();

    @Nonnull
    private static final Map<TextLanguage, Analyzer> STANDARD_ANALYZER_MAPPING_INSTANCE = getMappingForStandardAnalyzer();

    @Nonnull
    private final Map<String, LuceneAnalyzerFactory> registry;

    @Nonnull
    private static Map<String, LuceneAnalyzerFactory> initRegistry() {
        final Map<String, LuceneAnalyzerFactory> registry = new HashMap<>();
        for (LuceneAnalyzerFactory factory : ServiceLoader.load(LuceneAnalyzerFactory.class)) {
            final String name = factory.getName();
            if (registry.containsKey(name)) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("duplicate lucene analyzer", LuceneLogMessageKeys.ANALYZER_NAME, name));
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("found lucene analyzer", LuceneLogMessageKeys.ANALYZER_NAME, name));
                }
                registry.put(name, factory);
            }
        }
        return registry;
    }

    @Nonnull
    public static LuceneAnalyzerRegistry instance() {
        return INSTANCE;
    }

    @Nonnull
    public static Map<TextLanguage, Analyzer> standardAnalyzerMappingInstance() {
        return STANDARD_ANALYZER_MAPPING_INSTANCE;
    }

    private LuceneAnalyzerRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    @Override
    public Pair<Map<TextLanguage, Analyzer>, Map<TextLanguage, Analyzer>> getLuceneAnalyzerPair(@Nonnull Index index) {
        final String name = index.getOption(LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION);
        // TODO: Get rid of the condition after OR operator, after having all analyzers registered with this registry
        if (name == null || !registry.containsKey(name)) {
            return Pair.of(STANDARD_ANALYZER_MAPPING_INSTANCE, STANDARD_ANALYZER_MAPPING_INSTANCE);
        } else {
            LuceneAnalyzerFactory analyzerFactory = registry.get(name);
            if (analyzerFactory == null) {
                throw new MetaDataException("unrecognized lucene analyzer for tokenizer", LuceneLogMessageKeys.ANALYZER_NAME, name);
            }
            final Map<TextLanguage, Analyzer> indexAnalyzerMap = analyzerFactory.getIndexAnalyzerMap(index);
            return Pair.of(indexAnalyzerMap, analyzerFactory.getQueryAnalyzerMap(index, indexAnalyzerMap));
        }
    }

    private static Map<TextLanguage, Analyzer> getMappingForStandardAnalyzer() {
        Map<TextLanguage, Analyzer> map = new EnumMap<>(TextLanguage.class);
        for (TextLanguage language : TextLanguage.values()) {
            map.put(language, new StandardAnalyzer());
        }
        return map;
    }
}

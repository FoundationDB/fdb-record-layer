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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.exact.ExactTokenAnalyzerFactory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.apple.foundationdb.record.util.pair.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
    private final Map<LuceneAnalyzerType, Map<String, LuceneAnalyzerFactory>> registry;

    @Nonnull
    private static Map<LuceneAnalyzerType, Map<String, LuceneAnalyzerFactory>> initRegistry() {
        final Map<LuceneAnalyzerType, Map<String, LuceneAnalyzerFactory>> registry = new HashMap<>();
        for (LuceneAnalyzerFactory factory : ServiceLoaderProvider.load(LuceneAnalyzerFactory.class)) {
            final String name = factory.getName();
            final LuceneAnalyzerType type = factory.getType();
            if (registry.containsKey(type) && registry.get(type).containsKey(name)) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("duplicate lucene analyzer",
                            LuceneLogMessageKeys.ANALYZER_NAME, name,
                            LuceneLogMessageKeys.ANALYZER_TYPE, type.name()));
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.of("found lucene analyzer",
                            LuceneLogMessageKeys.ANALYZER_NAME, name,
                            LuceneLogMessageKeys.ANALYZER_TYPE, type.name()));
                }
                registry.putIfAbsent(type, new HashMap<>());
                registry.get(type).put(name, factory);
            }
        }
        return registry;
    }

    @SpotBugsSuppressWarnings(value = "MS_EXPOSE_REP", justification = "Object is actually immutable")
    @Nonnull
    public static LuceneAnalyzerRegistry instance() {
        return INSTANCE;
    }

    private LuceneAnalyzerRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    @Override
    public LuceneAnalyzerCombinationProvider getLuceneAnalyzerCombinationProvider(@Nonnull final Index index,
                                                                                  @Nonnull final LuceneAnalyzerType type,
                                                                                  @Nonnull final Map<String, LuceneIndexExpressions.DocumentFieldDerivation> auxiliaryFieldInfo) {
        final String defaultAnalyzerName = index.getOption(type.getAnalyzerOptionKey());
        final String analyzerPerFieldName = index.getOption(type.getAnalyzerPerFieldOptionKey());
        Pair<AnalyzerChooser, AnalyzerChooser> defaultAnalyzerChooserPair = getAnalyzerChooser(index, defaultAnalyzerName, type);

        Map<String, AnalyzerChooser> indexAnalyzerChooserPerFieldOverride = new TreeMap<>();
        Map<String, AnalyzerChooser> queryAnalyzerChooserPerFieldOverride = new TreeMap<>();

        if (analyzerPerFieldName != null) {
            LuceneIndexOptions.parseKeyValuePairOptionValue(analyzerPerFieldName).forEach((fieldName, analyzerName) -> {
                Pair<AnalyzerChooser, AnalyzerChooser> perFieldAnalyzerChooserPair = getAnalyzerChooser(index, analyzerName, type);
                indexAnalyzerChooserPerFieldOverride.put(fieldName, perFieldAnalyzerChooserPair.getLeft());
                queryAnalyzerChooserPerFieldOverride.put(fieldName, perFieldAnalyzerChooserPair.getRight());
            });
        }

        auxiliaryFieldInfo.forEach((fieldName, fieldInfo) -> {
            addPerFieldAnalyzerIfNecessary(indexAnalyzerChooserPerFieldOverride, fieldName, fieldInfo, index, type);
            addPerFieldAnalyzerIfNecessary(queryAnalyzerChooserPerFieldOverride, fieldName, fieldInfo, index, type);
        });

        return new LuceneAnalyzerCombinationProvider(defaultAnalyzerChooserPair.getLeft(), defaultAnalyzerChooserPair.getRight(),
                indexAnalyzerChooserPerFieldOverride, queryAnalyzerChooserPerFieldOverride);
    }

    private void addPerFieldAnalyzerIfNecessary(@Nonnull final Map<String, AnalyzerChooser> chooserPerFieldOverride,
                                                @Nonnull final String fieldName,
                                                @Nonnull final LuceneIndexExpressions.DocumentFieldDerivation fieldInfo,
                                                @Nonnull final Index index,
                                                @Nonnull final LuceneAnalyzerType type) {
        if (chooserPerFieldOverride.containsKey(fieldName)) {
            return; // do not override already chosen field analyzer.
        }
        if (type != LuceneAnalyzerType.FULL_TEXT) {
            return; // not sure how to deal with other types (i.e. AUTO_COMPLETE) yet.
        }
        if (isEligibleForNoOpAnalyzer(fieldInfo)) {
            if (registry.isEmpty()
                    || registry.get(type) == null
                    || registry.get(type).get(ExactTokenAnalyzerFactory.NAME) == null) {
                throw new MetaDataException("could not retrieve analyzer",
                        LuceneLogMessageKeys.ANALYZER_NAME, ExactTokenAnalyzerFactory.NAME,
                        LuceneLogMessageKeys.ANALYZER_TYPE, type);
            }
            chooserPerFieldOverride.put(fieldName, registry.get(type).get(ExactTokenAnalyzerFactory.NAME).getIndexAnalyzerChooser(index));
        }
    }

    private static boolean isEligibleForNoOpAnalyzer(@Nonnull final LuceneIndexExpressions.DocumentFieldDerivation fieldInfo) {
        return fieldInfo.getType() != LuceneIndexExpressions.DocumentFieldType.TEXT;
    }

    private Pair<AnalyzerChooser, AnalyzerChooser> getAnalyzerChooser(@Nonnull Index index, @Nullable String analyzerName, @Nonnull LuceneAnalyzerType type) {
        final Map<String, LuceneAnalyzerFactory> registryForType = Objects.requireNonNullElse(registry.get(type), Collections.emptyMap());
        if (analyzerName == null || !registryForType.containsKey(analyzerName)) {
            return Pair.of(t -> LuceneAnalyzerWrapper.getStandardAnalyzerWrapper(),
                    t -> LuceneAnalyzerWrapper.getStandardAnalyzerWrapper());
        } else {
            LuceneAnalyzerFactory analyzerFactory = registryForType.get(analyzerName);
            if (analyzerFactory == null) {
                throw new MetaDataException("unrecognized lucene analyzer for tokenizer",
                        LuceneLogMessageKeys.ANALYZER_NAME, analyzerName,
                        LuceneLogMessageKeys.ANALYZER_TYPE, type.name());
            }
            final AnalyzerChooser indexAnalyzerChooser = analyzerFactory.getIndexAnalyzerChooser(index);
            return Pair.of(indexAnalyzerChooser, analyzerFactory.getQueryAnalyzerChooser(index, indexAnalyzerChooser));
        }
    }
}

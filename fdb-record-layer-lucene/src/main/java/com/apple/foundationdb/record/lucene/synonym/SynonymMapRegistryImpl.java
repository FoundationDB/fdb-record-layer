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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Registry for {@link SynonymMap}s.
 */
public class SynonymMapRegistryImpl implements SynonymMapRegistry {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(SynonymMapRegistryImpl.class);
    private static final SynonymMapRegistryImpl INSTANCE = new SynonymMapRegistryImpl();

    @Nonnull
    private final Map<String, SynonymMap> registry;

    private SynonymMapRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    public static SynonymMapRegistry instance() {
        return INSTANCE;
    }

    @Nonnull
    @Override
    public SynonymMap getSynonymMap(@Nonnull final String name) {
        final SynonymMap map = registry.get(name);
        if (map == null) {
            throw new MetaDataException("unrecognized synonym map", LogMessageKeys.SYNONYM_NAME, name);
        }
        return map;
    }

    @Nonnull
    private static Map<String, SynonymMap> initRegistry() {
        final Map<String, SynonymMap> registry = new HashMap<>();
        for (SynonymMapConfig config : ServiceLoader.load(SynonymMapConfig.class)) {
            if (registry.containsKey(config.getName())) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("duplicate synonym map", LogMessageKeys.SYNONYM_NAME, config.getName()));
                }
            }
            registry.put(config.getName(), buildSynonymMap(config));
        }
        return registry;
    }

    private static SynonymMap buildSynonymMap(final SynonymMapConfig config) {
        try {
            SynonymMap.Parser parser = new SolrSynonymParser(true, true, new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    final StandardTokenizer src = new StandardTokenizer();
                    TokenStream tok = new LowerCaseFilter(src);
                    tok = new FlattenGraphFilter(tok);
                    return new TokenStreamComponents(src, tok);
                }
            });
            parser.parse(new InputStreamReader(config.getSynonymInputStream(), StandardCharsets.UTF_8));
            return parser.build();
        } catch (IOException | ParseException ex) {
            throw new RecordCoreException("Failed to build synonym map", ex)
                    .addLogInfo(LogMessageKeys.SYNONYM_NAME, config.getName());
        }
    }
}

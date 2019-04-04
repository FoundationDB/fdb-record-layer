/*
 * TextTokenizerRegistryImpl.java
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexMaintainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Default implementation of the {@link TextTokenizerRegistry}. It uses the class
 * loader to determine which {@link TextTokenizerFactory} implementation exist,
 * and it populates the registry with those tokenizers. An instance of this registry
 * is used by the {@link TextIndexMaintainer}
 * in order to choose the tokenizer for a block of text. One can therefore register
 * additional tokenizers for that index by calling the {@link #register(TextTokenizerFactory) register}
 * method on the singleton instance of this class and supplying the additional tokenizer.
 */
@API(API.Status.EXPERIMENTAL)
public class TextTokenizerRegistryImpl implements TextTokenizerRegistry {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(TextTokenizerRegistryImpl.class);
    @Nonnull
    private static final TextTokenizerRegistryImpl INSTANCE = new TextTokenizerRegistryImpl();

    @Nonnull
    private Map<String, TextTokenizerFactory> registry;

    @Nonnull
    private static Map<String, TextTokenizerFactory> initRegistry() {
        final Map<String, TextTokenizerFactory> registry = new HashMap<>();
        for (TextTokenizerFactory factory : ServiceLoader.load(TextTokenizerFactory.class)) {
            final String name = factory.getName();
            if (registry.containsKey(name)) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("duplicate text tokenizer", LogMessageKeys.TOKENIZER_NAME, name));
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("found text tokenizer", LogMessageKeys.TOKENIZER_NAME, name));
                }
                registry.put(name, factory);
            }
        }
        return registry;
    }

    @Nonnull
    public static TextTokenizerRegistry instance() {
        return INSTANCE;
    }

    private TextTokenizerRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    @Override
    public Map<String, TextTokenizerFactory> getRegistry() {
        return Collections.unmodifiableMap(registry);
    }

    @Nonnull
    @Override
    public TextTokenizer getTokenizer(@Nullable String name) {
        if (name == null) {
            return DefaultTextTokenizer.instance();
        } else {
            TextTokenizerFactory tokenizerFactory = registry.get(name);
            if (tokenizerFactory == null) {
                throw new MetaDataException("unrecognized text tokenizer", LogMessageKeys.TOKENIZER_NAME, name);
            }
            return tokenizerFactory.getTokenizer();
        }
    }

    // Synchronize this method so that we don't need a ConcurrentHashMap but so that
    // it is still thread safe.
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public synchronized void register(@Nonnull TextTokenizerFactory tokenizerFactory) {
        final String name = tokenizerFactory.getName();
        TextTokenizerFactory oldFactory = registry.putIfAbsent(name, tokenizerFactory);
        // If there was a factory already registered and the old factory isn't the same as
        // the new one, throw an error
        if (oldFactory != null && oldFactory != tokenizerFactory) {
            throw new RecordCoreArgumentException("attempted to register duplicate tokenizer", LogMessageKeys.TOKENIZER_NAME, name);
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("registered tokenizer", LogMessageKeys.TOKENIZER_NAME, name));
        }
    }

    @Override
    public void reset() {
        registry = initRegistry();
    }
}

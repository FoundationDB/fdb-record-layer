/*
 * TestingIndexMaintainerRegistry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * A testing-oriented version of {@link IndexMaintainerRegistry} that allows overriding a registry.
 * This can be used in place of the production registry
 */
public class TestingIndexMaintainerRegistry implements IndexMaintainerFactoryRegistry {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(TestingIndexMaintainerRegistry.class);

    @Nonnull
    private final Map<String, IndexMaintainerFactory> registry = new HashMap<>();

    public TestingIndexMaintainerRegistry() {
        initRegistry();
    }

    @Nonnull
    @Override
    public IndexMaintainerFactory getIndexMaintainerFactory(@Nonnull final Index index) {
        final IndexMaintainerFactory factory = registry.get(index.getType());
        if (factory == null) {
            throw new MetaDataException("Unknown index type for " + index);
        }
        return factory;
    }

    /**
     * The method that can be used by tests to override an implementation of the factory by another.
     * @param factory the factory to register
     */
    public void overrideFactory(IndexMaintainerFactory factory) {
        registerFactory(factory, true);
    }

    private void initRegistry() {
        for (IndexMaintainerFactory factory : ServiceLoaderProvider.load(IndexMaintainerFactory.class)) {
            registerFactory(factory, false);
        }
    }

    private void registerFactory(final IndexMaintainerFactory factory, final boolean allowOverride) {
        for (String type : factory.getIndexTypes()) {
            if (registry.containsKey(type) && !allowOverride) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.of("duplicate index maintainer", LogMessageKeys.INDEX_TYPE, type));
                }
            } else {
                registry.put(type, factory);
            }
        }
    }
}

/*
 * IndexMaintainerRegistryImpl.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.indexes.SlidingWindowIndexMaintainerFactory;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * A singleton {@link IndexMaintainerRegistry} that finds {@link IndexMaintainerFactory} classes in the classpath.
 */
@API(API.Status.INTERNAL)
public class IndexMaintainerFactoryRegistryImpl implements IndexMaintainerFactoryRegistry {
    @Nonnull
    protected static final IndexMaintainerFactoryRegistryImpl INSTANCE = new IndexMaintainerFactoryRegistryImpl();

    @Nonnull
    private final Map<String, IndexMaintainerFactory> registry;

    @Nonnull
    public static IndexMaintainerFactoryRegistry instance() {
        return INSTANCE;
    }

    @Nonnull
    protected static Map<String, IndexMaintainerFactory> initRegistry() {
        return buildRegistry(ServiceLoaderProvider.load(IndexMaintainerFactory.class));
    }

    /**
     * Maps each index type to the factory that maintains it. An index type may be claimed by only one factory.
     *
     * @param factories the factories found on the class path
     *
     * @return the index type to factory map
     *
     * @throws RecordCoreException if two factories claim the same index type
     */
    @Nonnull
    @VisibleForTesting
    static Map<String, IndexMaintainerFactory> buildRegistry(@Nonnull final Iterable<IndexMaintainerFactory> factories) {
        final Map<String, IndexMaintainerFactory> registry = new HashMap<>();
        for (IndexMaintainerFactory factory : factories) {
            for (String type : factory.getIndexTypes()) {
                final var existingFactory = registry.put(type, factory);
                if (existingFactory != null) {
                    throw new RecordCoreException("duplicate index maintainer factory for index type",
                            LogMessageKeys.INDEX_TYPE, type,
                            LogMessageKeys.VALUE, existingFactory.getClass().getName() + ", "
                                                  + factory.getClass().getName());
                }
            }
        }
        return registry;
    }

    protected IndexMaintainerFactoryRegistryImpl() {
        registry = initRegistry();
    }

    @Nonnull
    @Override
    public IndexMaintainerFactory getIndexMaintainerFactory(@Nonnull final Index index) {
        final IndexMaintainerFactory factory = registry.get(index.getType());
        if (factory == null) {
            throw new MetaDataException("Unknown index type for " + index);
        }
        // sliding window indexes are not composable
        if (SlidingWindowIndexMaintainerFactory.isSlidingWindowIndex(index)) {
            return new SlidingWindowIndexMaintainerFactory(factory);
        }
        return factory;
    }
}

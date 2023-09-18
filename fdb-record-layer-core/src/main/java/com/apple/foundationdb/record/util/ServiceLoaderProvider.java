/*
 * ServiceLoaderProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * A singleton that supports alternatives to {@link java.util.ServiceLoader}, for example if you want to
 * use a dependency injection framework.
 */
public final class ServiceLoaderProvider {
    public static final Function<Class<?>, Iterable<?>> DEFAULT_LOADER = ServiceLoader::load;
    private static Function<Class<?>, Iterable<?>> temp = DEFAULT_LOADER;

    private static final class Holder {
        static final Function<Class<?>, Iterable<?>> serviceLoader = ServiceLoaderProvider.temp;
    }

    private ServiceLoaderProvider() {
        // Singleton utility class
    }

    /**
     * Replaces the default service loader.
     * This must be called BEFORE ServiceLoaderProvider.load() is called.
     *
     * @param serviceLoader Replacement service loader.
     * @throws IllegalStateException if called while the application is already running
     */
    public static void initialize(final Function<Class<?>, Iterable<?>> serviceLoader) {
        temp = serviceLoader;
        if (!Holder.serviceLoader.equals(serviceLoader)) {
            throw new IllegalStateException("ServiceLoaderProvider already initialized");
        }
    }

    /**
     * Creates an Iterable of services instances for the given service.
     *
     * @param clazz class
     * @param <T> type
     * @return service loader
     */
    @SuppressWarnings("unchecked")
    public static <T> Iterable<T> load(final Class<T> clazz) {
        return (Iterable<T>) Holder.serviceLoader.apply(clazz);
    }
}

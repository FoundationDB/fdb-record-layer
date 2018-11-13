/*
 * ResolverCreateHooks.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Set of hooks to be run <b>only</b> when creating a mapping in a {@link LocatableResolver}. Contains two components: one
 * {@link MetadataHook} which will specify the metadata to add to the mapping at create time, and a set of
 * {@link PreWriteCheck} which can be used to check that the database is in a consistent state that allows for writes.
 */
@API(API.Status.EXPERIMENTAL)
public class ResolverCreateHooks {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResolverCreateHooks.class);
    public static final PreWriteCheck DEFAULT_CHECK = (ignore1, ignore2) -> AsyncUtil.READY_TRUE;
    public static final MetadataHook DEFAULT_HOOK = ignore -> null;
    @Nonnull
    private final List<PreWriteCheck> checks;
    @Nonnull
    private final MetadataHook metadataHook;

    public ResolverCreateHooks(@Nonnull PreWriteCheck check,
                               @Nonnull MetadataHook metadataHook) {
        this(ImmutableList.of(check), metadataHook);
    }

    public ResolverCreateHooks(@Nonnull List<PreWriteCheck> checks,
                               @Nonnull MetadataHook metadataHook) {
        this.checks = checks;
        this.metadataHook = metadataHook;
    }

    public static ResolverCreateHooks getDefault() {
        return new ResolverCreateHooks(DEFAULT_CHECK, DEFAULT_HOOK);
    }

    /**
     * Gets a set of hooks for checking the migration state of the global {@link LocatableResolver}. Does not set a
     * metadata hook.
     * @return the {@link ResolverCreateHooks}
     */
    public static ResolverCreateHooks getMigratableGlobal() {
        List<PreWriteCheck> checks = ImmutableList.of((context, providedResolver) ->
                GlobalResolverFactory.globalResolverNoCache(context)
                        .thenApply(resolver -> {
                            if (!Objects.equals(resolver, providedResolver)) {
                                LOGGER.warn(KeyValueLogMessage.of("LocatableResolver inconsistent with cached state",
                                        "cachedResolver", providedResolver,
                                        "readResolver", resolver));
                                return false;
                            }
                            return true;
                        })
        );
        return new ResolverCreateHooks(checks, DEFAULT_HOOK);
    }

    @Nonnull
    public List<PreWriteCheck> getPreWriteChecks() {
        return checks;
    }

    @Nonnull
    public MetadataHook getMetadataHook() {
        return metadataHook;
    }

    /**
     * Functional interface for the safety check that will be run before writing a mapping in the
     * {@link LocatableResolver}. The passed {@link LocatableResolver} will be the resolver we are attempting to
     * write to. Implementations can then use the transaction provided to check whether that resolver is the
     * correct one. If not, implementations must throw an exception.
     */
    @FunctionalInterface
    public interface PreWriteCheck extends BiFunction<FDBRecordContext, LocatableResolver, CompletableFuture<Boolean>> {
    }

    /**
     * Functional interface for the safety check that will be run before writing a mapping in the {@link LocatableResolver}.
     */
    @FunctionalInterface
    public interface MetadataHook extends Function<String, byte[]> {
    }
}

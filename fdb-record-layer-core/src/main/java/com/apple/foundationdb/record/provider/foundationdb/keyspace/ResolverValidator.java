/*
 * ResolverValidator.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;


import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBReverseDirectoryCache;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Validator that ensures that the resolver mappings are consistent with the reverse directory mappings.  Currently
 * this is only uni-directional, meaning that it only ensures that the main mapping has a corresponding reverse
 * mapping and that the values match.
 */
@API(API.Status.EXPERIMENTAL)
public class ResolverValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResolverValidator.class);

    /**
     * Exhaustively validate entries in a {@link LocatableResolver}'s mappings, ensuing that all entries in the mapping
     * have corresponding and correct entries in the reverse mapping.  This method will create as many transactions as
     * necessary to perform the validation.
     *
     * @param timer store timer collecting metrics about the operation
     * @param resolver the resolver to be validated
     * @param executeProperties template of execution properties that will be applied to each transaction
     *   involvaed in the validation
     * @param reverseLookupPipelineSize number of reverse directory lookups to perform in parallel while scanning
     *   the forward entries
     * @param badEntriesOnly if true, only invalid entries will be handed to the {@code entryListener}
     * @param entryListener consumer that will be handed validated entries
     */
    public static void validate(@Nullable final FDBStoreTimer timer,
                                @Nonnull final LocatableResolver resolver,
                                @Nonnull final ExecuteProperties.Builder executeProperties,
                                final int reverseLookupPipelineSize,
                                final boolean badEntriesOnly,
                                @Nonnull final Consumer<ValidatedEntry> entryListener) {
        try (RecordCursor<ValidatedEntry> cursor = new AutoContinuingCursor<>(
                resolver.getDatabase().newRunner(),
                (context, continuation) ->
                    validate(resolver, context, continuation, reverseLookupPipelineSize, false,
                            new ScanProperties(executeProperties.build())),
                3)) {
            resolver.getDatabase().asyncToSync(timer, FDBStoreTimer.Waits.WAIT_VALIDATE_RESOLVER,
                    cursor.forEach(validatedEntry -> {
                        if (!badEntriesOnly || validatedEntry.getValidationResult() != ValidationResult.OK) {
                            entryListener.accept(validatedEntry);
                        }
                    }));
        }
    }

    /**
     * Validate entries in a {@link LocatableResolver}'s mappings. This method scans the forward mapping
     * ({@code String} -&gt; {@code long}) and validates that the entry has a corresponding entry in the
     * reverse mapping, as well as validating that the entry in the reverse mapping points to the forward
     * mapping key.
     *
     * @param resolver the resolver to scan
     * @param context the transaction context to use
     * @param continuation continuation from a previous scan
     * @param repairMissingEntries if {@code true}, missing reverse directory entries will be restored
     * @param scanProperties scan properties that control the nature of the scan
     * @return a cursor returning the disposition of all scanned entries in the resolvers mapping table
     */
    public static RecordCursor<ValidatedEntry> validate(@Nonnull final LocatableResolver resolver,
                                                        @Nonnull final FDBRecordContext context,
                                                        @Nullable final byte[] continuation,
                                                        final boolean repairMissingEntries,
                                                        @Nonnull final ScanProperties scanProperties) {
        return validate(resolver, context, continuation, 10, repairMissingEntries, scanProperties);
    }

    /**
     * Validate entries in a {@link LocatableResolver}'s mappings. This method scans the forward mapping
     * ({@code String} -&gt; {@code long}) and validates that the entry has a corresponding entry in the
     * reverse mapping, as well as validating that the entry in the reverse mapping points to the forward
     * mapping key. For example, if {@code "foo" -&gt; 10} is in the forward mapping and {@code "foo"} is
     * not found in the reverse mapping, it will be returned as a bad entry. Similarly, if {@code "foo"} is
     * found in the reverse mapping, but it has a value of {@code 30}, then that is considered an error.
     *
     * @param resolver the resolver to scan
     * @param context the transaction context to use
     * @param continuation continuation from a previous scan
     * @param reverseLookupPipelineSize the number of reverse directory lookups to perform in parallel while
     *   scanning the forward mappings
     * @param repairMissingEntries if {@code true}, missing reverse directory entries will be restored
     * @param scanProperties scan properties that control the nature of the scan
     * @return a cursor returning the disposition of all scanned entries in the resolvers mapping table
     */
    public static RecordCursor<ValidatedEntry> validate(@Nonnull final LocatableResolver resolver,
                                                        @Nonnull final FDBRecordContext context,
                                                        @Nullable final byte[] continuation,
                                                        final int reverseLookupPipelineSize,
                                                        final boolean repairMissingEntries,
                                                        @Nonnull final ScanProperties scanProperties) {
        return resolver.scan(context, continuation, scanProperties)
                .mapPipelined(keyValue -> {
                    // The reverse directory cache has an entry in the directory layer that it does not,
                    // itself maintain a reverse entry for (it probably SHOULD, but we now have caches that
                    // were created that way and have to accept it.
                    if (keyValue.getKey().equals(FDBReverseDirectoryCache.REVERSE_DIRECTORY_CACHE_ENTRY)) {
                        return CompletableFuture.completedFuture(new ValidatedEntry(ValidationResult.OK, keyValue));
                    }

                    return resolver.reverseLookup(context.getTimer(), keyValue.getValue().getValue())
                            .handle((reverseKey, exception) -> {
                                if (exception != null) {
                                    if (isEntryMissing(exception)) {
                                        return new ValidatedEntry(ValidationResult.REVERSE_ENTRY_MISSING, keyValue);
                                    }

                                    throw new RecordCoreException("Error reading reverse directory entry", exception)
                                            .addLogInfo(LogMessageKeys.RESOLVER, resolver)
                                            .addLogInfo(LogMessageKeys.RESOLVER_KEY, keyValue.getKey())
                                            .addLogInfo(LogMessageKeys.RESOLVER_VALUE, keyValue.getValue().getValue())
                                            .addLogInfo(LogMessageKeys.RESOLVER_METADATA, ByteArrayUtil2.loggable(keyValue.getValue().getMetadata()));
                                }

                                if (!reverseKey.equals(keyValue.getKey())) {
                                    return new ValidatedEntry(ValidationResult.REVERSE_ENTRY_MISMATCH, keyValue, reverseKey);
                                }

                                return new ValidatedEntry(ValidationResult.OK, keyValue);
                            }).thenCompose(validatedEntry -> {
                                if (repairMissingEntries
                                        && validatedEntry.getValidationResult() == ValidationResult.REVERSE_ENTRY_MISSING) {
                                    return repairMissingEntry(resolver, context, validatedEntry);
                                }
                                return CompletableFuture.completedFuture(validatedEntry);
                            });
                },
                reverseLookupPipelineSize);
    }

    private static CompletableFuture<ValidatedEntry> repairMissingEntry(@Nonnull LocatableResolver resolver,
                                                                        @Nonnull FDBRecordContext context,
                                                                        @Nonnull ValidatedEntry validatedEntry) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("Restoring missing reverse mapping",
                    LogMessageKeys.RESOLVER, resolver,
                    LogMessageKeys.RESOLVER_KEY, validatedEntry.getKey(),
                    LogMessageKeys.RESOLVER_VALUE, validatedEntry.getValue().getValue()));
        }

        return resolver.putReverse(context, validatedEntry.getValue().getValue(), validatedEntry.getKey())
                .thenApply(vignore -> validatedEntry);
    }

    private static boolean isEntryMissing(Throwable e) {
        if (e instanceof CompletionException) {
            e = e.getCause();
        }
        return e instanceof NoSuchElementException;
    }

    /**
     * The type of bad entry discovered when validating a {@code LocatableResolver}'s mappings.
     */
    public enum ValidationResult {
        /**
         * Entry is valid.
         */
        OK,

        /**
         * Indicates that an entry in the mapping has no corresponding reverse entry.
         */
        REVERSE_ENTRY_MISSING,

        /**
         * Indicates that an entry in the mapping has a corresponding reverse entry, but that reverse entry
         * belong to a different key than the one that referenced it.
         */
        REVERSE_ENTRY_MISMATCH
    }

    /**
     * Represents a bad/incorrect entry in the resolvers mapping.
     */
    public static class ValidatedEntry {
        @Nonnull
        private final ValidationResult result;
        @Nonnull
        private final ResolverKeyValue keyValue;
        @Nonnull
        private final String reverseValue;

        public ValidatedEntry(@Nonnull final ValidationResult result,
                              @Nonnull final ResolverKeyValue keyValue) {
            this(result, keyValue, keyValue.getKey());
        }

        public ValidatedEntry(@Nonnull final ValidationResult result,
                              @Nonnull final ResolverKeyValue keyValue,
                              @Nonnull final String reverseValue) {
            this.result = result;
            this.keyValue = keyValue;
            this.reverseValue = reverseValue;
        }

        @Nonnull
        public ValidationResult getValidationResult() {
            return result;
        }

        /**
         * Return the key in the resolver mapping.
         * @return the key in the resolver mapping
         */
        @Nonnull
        public String getKey() {
            return keyValue.getKey();
        }

        /**
         * The value for the key in the mapping.
         * @return the value for the key in the mapping
         */
        @Nonnull
        public ResolverResult getValue() {
            return keyValue.getValue();
        }

        /**
         * The key found in the reverse directory associated with the value found in the forward mapping.
         * For {@link ValidationResult#REVERSE_ENTRY_MISMATCH} this will be a different value than returned
         * by {@link #getKey()}.
         *
         * @return the key found in the reverse directory associated with the value found in the forward mapping
         */
        @Nonnull
        public String getReverseValue() {
            return reverseValue;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ValidatedEntry validatedEntry = (ValidatedEntry)o;
            return result == validatedEntry.result && keyValue.equals(validatedEntry.keyValue) && Objects.equals(reverseValue, validatedEntry.reverseValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result, keyValue, reverseValue);
        }

        @Override
        public String toString() {
            return "BadEntry{" +
                   "error=" + result +
                   ", keyValue=" + keyValue +
                   ", reverseValue=" + reverseValue +
                   '}';
        }
    }
}

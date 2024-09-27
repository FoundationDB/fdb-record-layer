/*
 * MinimumTupleSizeKeyChecker.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.TupleHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Key checker that reports when operations are not limited to a single tuple subspace of a given minimum size.
 */
public class MinimumTupleSizeKeyChecker implements KeyChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinimumTupleSizeKeyChecker.class);

    private final List<CheckedSubspace> checks;

    private final List<byte[]> prefixes = new ArrayList<>();

    /**
     * A key prefix and associated check settings.
     * Only the first matching prefix will be used.
     */
    public static class CheckedSubspace {
        @Nonnull
        private final byte[] prefix;
        private final boolean dryRun;
        private final boolean checkReads;
        private final int minTupleSize;
        private final int maxTuplePrefixCount;

        /**
         * Create a new checked subspace.
         * @param prefix key prefix to which these checks apply
         * @param dryRun whether to throw or just log problems
         * @param checkReads whether to also check read operations
         * @param minTupleSize minimum size (number of elements) of tuple-decoded keys
         * @param maxTuplePrefixCount maximum number of different prefixes
         */
        public CheckedSubspace(@Nonnull byte[] prefix,
                               boolean dryRun, boolean checkReads,
                               int minTupleSize, int maxTuplePrefixCount) {
            this.prefix = prefix;
            this.dryRun = dryRun;
            this.checkReads = checkReads;
            this.minTupleSize = minTupleSize;
            this.maxTuplePrefixCount = maxTuplePrefixCount;
        }
    }

    /**
     * Create a new checker.
     * @param checks ordered list of checks to match and apply
     */
    public MinimumTupleSizeKeyChecker(@Nonnull List<CheckedSubspace> checks) {
        this.checks = checks;
    }

    @Override
    public void checkKey(final byte[] key, final boolean write) {
        final CheckedSubspace checkedSubspace = findCheckedSubspace(key);
        if (checkedSubspace == null) {
            return;
        }
        if (!checkedSubspace.checkReads && !write) {
            return;
        }
        checkKeyLength(checkedSubspace, key);
    }

    @Override
    public void checkKeyRange(final byte[] keyBegin, final byte[] keyEnd, final boolean write) {
        final CheckedSubspace checkedSubspace = findCheckedSubspace(keyBegin);
        if (checkedSubspace == null) {
            return;
        }
        if (!checkedSubspace.checkReads && !write) {
            return;
        }
        final byte[] prefix = checkKeyLength(checkedSubspace, keyBegin);
        if (prefix != null) {
            if (!keyHasPrefix(keyEnd, prefix)) {
                checkFails(checkedSubspace, "key range not limited to single subspace",
                        "keyBegin", ByteArrayUtil2.loggable(keyBegin),
                        "keyEnd", ByteArrayUtil2.loggable(keyEnd));
            }
        }
    }

    @Override
    public void close() {
        for (CheckedSubspace checkedSubspace : checks) {
            if (checkedSubspace.dryRun) {
                checkPrefixesCount(checkedSubspace);
            }
        }
    }

    @Nullable
    private CheckedSubspace findCheckedSubspace(@Nonnull byte[] key) {
        for (CheckedSubspace checkedSubspace : checks) {
            if (keyHasPrefix(key, checkedSubspace.prefix)) {
                return checkedSubspace;
            }
        }
        return null;
    }

    @Nullable
    private synchronized byte[] checkKeyLength(@Nonnull CheckedSubspace checkedSubspace, final byte[] key) {
        final int prefixLength = TupleHelpers.prefixLengthOfSize(key, checkedSubspace.minTupleSize);
        if (prefixLength < 0) {
            checkFails(checkedSubspace, "key not long enough", "key", ByteArrayUtil2.loggable(key));
            return null;
        }
        for (byte[] prefix : prefixes) {
            if (prefix.length == prefixLength &&
                    Arrays.equals(key, 0, prefixLength, prefix, 0, prefixLength)) {
                return prefix;
            }
        }
        final byte[] prefix = Arrays.copyOfRange(key, 0, prefixLength);
        prefixes.add(prefix);
        if (!checkedSubspace.dryRun) {
            checkPrefixesCount(checkedSubspace);
        }
        return prefix;
    }

    private static boolean keyHasPrefix(@Nonnull byte[] key, @Nonnull byte[] prefix) {
        return key.length >= prefix.length &&
                Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    private synchronized void checkPrefixesCount(@Nonnull CheckedSubspace checkedSubspace) {
        int count = 0;
        for (byte[] prefix : prefixes) {
            if (keyHasPrefix(prefix, checkedSubspace.prefix)) {
                count++;
            }
        }
        if (count > checkedSubspace.maxTuplePrefixCount) {
            final String loggable = prefixes.stream().map(ByteArrayUtil2::loggable).collect(Collectors.joining("\n"));
            checkFails(checkedSubspace, "too many subspaces", "prefixes", loggable);
        }
    }

    private void checkFails(@Nonnull CheckedSubspace checkedSubspace,
                            @Nonnull String message, @Nonnull Object... keysAndValues) {
        if (checkedSubspace.dryRun) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.of(message, keysAndValues), new Throwable("not thrown"));
            }
        } else {
            throw new RecordCoreException(message).addLogInfo(keysAndValues);
        }
    }
}

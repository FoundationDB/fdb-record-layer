/*
 * FDBDirectoryUtils.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCompoundReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

import javax.annotation.Nonnull;

/**
 * Utilities for standardizing some interactions with {@link FDBDirectory}.
 */
public class FDBDirectoryUtils {

    private FDBDirectoryUtils() {
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    public static FDBDirectoryBase getFDBDirectory(@Nonnull Directory directory) {
        final Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FDBDirectoryBase) {
            return ((FDBDirectoryBase)unwrapped);
        }
        if (unwrapped instanceof LuceneOptimizedCompoundReader) {
            return ((FDBDirectoryBase)FilterDirectory.unwrap(((LuceneOptimizedCompoundReader)unwrapped).getDirectory()));
        }
        throw new RecordCoreException("Unexpected type of directory " + unwrapped.getClass().getSimpleName())
                .addLogInfo(LuceneLogMessageKeys.NAME, unwrapped.getClass().getSimpleName());
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    public static FDBDirectoryBase getFDBDirectoryNotCompound(@Nonnull Directory directory) {
        final Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FDBDirectoryBase) {
            return ((FDBDirectoryBase)unwrapped);
        }
        if (unwrapped instanceof LuceneOptimizedCompoundReader) {
            throw new RecordCoreException("CompoundReader given as directory");
        }
        throw new RecordCoreException("Unexpected type of directory")
                .addLogInfo(LuceneLogMessageKeys.NAME, unwrapped.getClass().getSimpleName());
    }
}

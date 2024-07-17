/*
 * LuceneLogMessageKeys.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import java.util.Locale;

/**
 * Lucene specific logging keys.
 */
@API(API.Status.UNSTABLE)
public enum LuceneLogMessageKeys {
    // Lucene data compression
    ANALYZER_NAME,
    ANALYZER_TYPE,
    BLOCK_CACHE_STATS,
    BLOCK_NUMBER,
    BYTE_NUMBER,
    COMPRESSED_EVENTUALLY,
    COMPRESSION_SUPPOSED,
    COMPRESSION_VERSION,
    CHECKSUM,
    CURRENT_BLOCK,
    DATA_SIZE,
    DATA_VALUE,
    DEST_FILE,
    DOC_ID,
    ENCODED_DATA_SIZE,
    ENCRYPTED_EVENTUALLY,
    ENCRYPTION_SUPPOSED,
    FILE_ACTUAL_TOTAL_SIZE,
    FILE_COUNT,
    FILE_ID,
    FILE_LIST,
    FILE_NAME,
    FILE_PREFIX,
    FILE_REFERENCE,
    FILE_SUFFIX,
    FILE_TOTAL_SIZE,
    INITIAL_OFFSET,
    INPUT,
    LENGTH,
    LOCK_NAME,
    LOCK_UUID,
    LOCK_TIMESTAMP,
    LOCK_EXISTING_TIMESTAMP,
    LOCK_EXISTING_UUID,
    LOCK_DIRECTORY,
    MERGE_SOURCE,
    MERGE_TRIGGER,
    OFFSET,
    ORIGINAL_DATA_SIZE,
    POINTER,
    POSITION,
    REFERENCE_CACHE_STATUS,
    REF_ID,
    RESOURCE,
    SEEK_NUM,
    SEGMENT,

    //Lucene component
    COMPONENT,
    NAME,
    GROUP,
    INDEX_PARTITION,
    PARTITION_HIGH_WATERMARK,
    PARTITION_LOW_WATERMARK,
    RECORD_TIMESTAMP,
    COUNT,
    TOTAL_COUNT,
    PRIMARY_KEY,
    SEGMENTS,
    ;

    private final String logKey;

    LuceneLogMessageKeys() {
        this.logKey = name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return logKey;
    }
}

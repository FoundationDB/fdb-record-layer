/*
 * FDBRecordStoreProperties.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyKey;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

/**
 * Property keys for the {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore} and related classes.
 * These might affect details about how low-level operations are orchestrated. However, these properties should be
 * safe to flip at any time, so they should not affect the on-disk format in a way that is incompatible if multiple
 * instances operating on the same record store have different values set for the same property.
 */
@API(API.Status.EXPERIMENTAL)
public final class FDBRecordStoreProperties {
    /**
     * Whether single record deletes should unroll range deletes into multiple single key deletes. This is used
     * during {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#saveRecord(Message)} and
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#deleteRecord(Tuple)} to control
     * whether multiple FDB key clears are issued when a record is split across multiple keys, or if instead a single
     * range clear is issued that covers all keys for that record. Issuing multiple single-key clears can be
     * more efficient if the underlying FDB storage engine can more efficiently handle single-key clears.
     */
    public static final RecordLayerPropertyKey<Boolean> UNROLL_SINGLE_RECORD_DELETES = RecordLayerPropertyKey.booleanPropertyKey(
            "com.apple.foundationdb.record.recordstore.unroll_single_record_deletes", true);

    /**
     * Whether {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#loadRecord(Tuple) loading a record}
     * should use multiple single-key gets instead of a single scan. This option may help latencies and storage server
     * throughput on certain workloads when running with an LSM-based FDB storage engine. Note that records are stored
     * across multiple keys in the Record Layer to support
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion versionstamps} and records
     * larger than the {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#VALUE_SIZE_LIMIT max value size}.
     * Those keys are stored contiguously, and so they can be scanned with a single small range read, but if the data
     * are stored on an LSM, then the single scan may be less efficient than multiple gets.
     *
     * <p>
     * At the moment, this option is not recommended except during performance testing. Adopters should verify whether
     * this change increases their measured performance on their workload before using.
     * </p>
     */
    @API(API.Status.EXPERIMENTAL)
    public static final RecordLayerPropertyKey<Boolean> LOAD_RECORDS_VIA_GETS = RecordLayerPropertyKey.booleanPropertyKey(
            "com.apple.foundationdb.record.recordstore.load_records_via_gets", false);

    private FDBRecordStoreProperties() {
        throw new RecordCoreException("should not instantiate class of static prop");
    }
}

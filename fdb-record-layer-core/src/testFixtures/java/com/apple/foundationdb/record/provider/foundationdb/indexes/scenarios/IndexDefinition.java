/*
 * IndexDefinition.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.google.protobuf.Message;

import java.util.List;

public interface IndexDefinition {
    RecordMetaData getMetaData();

    List<Message> generateRecords(int count);

    /**
     * Generate records that are <em>not</em> covered by the index under test (for example records of a
     * different record type, or records that leave the indexed field unset). Saving these produces no
     * entries in the index, so they can be written alongside a scan to give a transaction writes without
     * touching the index's (possibly shared) secondary structures. Returns an empty list by default.
     *
     * @param count the number of records to generate
     * @return records that do not contribute entries to the index under test
     */
    default List<Message> generateOtherRecords(int count) {
        return List.of();
    }

    RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, ScanProperties scanProperties);

    String getIndexName();

    /**
     * Perform any one-time setup that the index requires before records can be saved. This is run
     * in its own transaction against a freshly opened store before the first records are written.
     * Most index types need nothing here; index types that require state to exist before indexing
     * (e.g. a time window leaderboard needs a window to be created) can override this.
     *
     * @param store the record store to set up
     */
    default void setupIndex(final FDBRecordStore store) {
        // no-op by default
    }

    /**
     * Determine whether two index scan results should be considered equal for the purposes of the
     * scenario assertions. By default this is an exact list equality, which is correct for index
     * types with a stable, fully-determined scan order. Index types whose scan results are not
     * guaranteed to be byte-identical between an incrementally maintained index and a bulk rebuild
     * may override this to relax the comparison.
     *
     * @param expected the expected scan result
     * @param actual the actual scan result
     * @return {@code true} if the two results are equivalent
     */
    default boolean scanResultsEqual(final List<IndexEntry> expected, final List<IndexEntry> actual) {
        return expected.equals(actual);
    }
}

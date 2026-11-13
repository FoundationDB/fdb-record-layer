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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;

import java.util.List;

/**
 * Describes a single index-under-test for the scenario framework. The framework (via
 * {@link IndexScenarioMetaData} and {@link ScenarioRecords}) owns the record metadata, the primary
 * key, and the grouping field, and wraps the definition's {@link #generateIndexedMessage} content
 * into a {@code ScenarioRecord}. A definition therefore only says <em>what</em> content to index
 * (in the generic {@code IndexedMessage}), <em>what</em> index to build over it, and <em>how</em>
 * to scan it. The framework also owns the index's name ({@link ScenarioRecords#INDEX_NAME}) and the
 * record type it is added to.
 */
public interface IndexDefinition {
    /**
     * Produce the indexable content for one generated message. The definition fills in only the
     * field(s) of the generic {@code IndexedMessage} that it indexes, with deterministic,
     * semantically relevant values (distinct/monotonic where the index requires it). The framework
     * wraps this into a {@code ScenarioRecord} with the primary key and grouping field.
     *
     * @param ordinal the 0-based position of the message among those generated for a scenario. It is
     * distinct for every message, but is not necessarily the record's primary key (records of an
     * unnested type carry several messages each).
     * @return the indexable content for that message
     */
    TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(int ordinal);

    /**
     * Build the index under test. The {@link IndexTarget} roots the definition's value expression
     * (written relative to an {@code IndexedMessage}) at the correct location for the current
     * scenario (normal / grouped / joined / unnested), and supplies the grouping prefix. The same
     * method therefore serves every scenario, including synthetic record types. The index must be
     * named {@link ScenarioRecords#INDEX_NAME}.
     *
     * @param target roots value expressions at the indexed message and supplies the grouping prefix
     * @return the index to add to the store
     */
    Index buildIndex(IndexTarget target);

    /**
     * Scan the entire index.
     *
     * @param store the record store to scan
     * @param index the index under test, as built by {@link #buildIndex}
     * @param scanProperties the properties to scan with
     * @return a cursor over every entry in the index
     */
    RecordCursor<IndexEntry> scanIndex(FDBRecordStore store, Index index, ScanProperties scanProperties);

    /**
     * Whether this index can be built with a leading {@code group} prefix usable by
     * {@code deleteRecordsWhere} (the {@code DeleteWhereGroup} scenario). Default {@code true}; a
     * definition overrides to {@code false} <em>with a documented reason</em> only when the
     * maintainer itself cannot support group deletion.
     *
     * @return whether the {@code DeleteWhereGroup} scenario applies
     */
    default boolean supportsGrouping() {
        return true;
    }

    /**
     * Whether this index can be built over a synthetic record type (the {@code SyntheticJoinedType}
     * and {@code SyntheticUnnestedType} scenarios). Default {@code true} (the unified
     * {@link #buildIndex} roots the same value expression under a synthetic constituent); a definition
     * overrides to {@code false} <em>with a documented reason</em> only when the maintainer itself
     * cannot support the index over a synthetic type.
     *
     * @return whether the synthetic-type scenarios apply
     */
    default boolean supportsSynthetic() {
        return true;
    }

    /**
     * Whether the index maintainer's scans honour a requested
     * {@link com.apple.foundationdb.record.IsolationLevel#SNAPSHOT} isolation level. Such a scan
     * performs only snapshot reads, and so adds no read-conflict ranges: another transaction can commit
     * changes to the index while the scan is in progress without causing the scanning transaction to
     * fail to commit. Return {@code false}, with the reason documented, for index types whose scans
     * always read at serializable isolation. For example, a Lucene scan reads the index's directory
     * metadata through ordinary (non-snapshot) reads whatever isolation level is requested.
     *
     * @return {@code true} if a snapshot-isolation scan of this index takes no read conflicts
     */
    default boolean supportsSnapshotIsolation() {
        return true;
    }

    /**
     * Apply any per-store configuration the index needs. This is called on every store the scenarios
     * open, immediately after {@code createOrOpen}, because these settings live on the store instance
     * rather than in the metadata. For example a Lucene index wants
     * {@code store.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false)} so that
     * merges do not run inline at commit time.
     *
     * @param store the record store to configure
     */
    default void configureStore(final FDBRecordStore store) {
        // no-op by default
    }

    /**
     * Perform any one-time setup that the index requires before records can be saved. This is run
     * in its own transaction against a freshly opened store before the first records are written.
     * Most index types need nothing here; index types that require state to exist before indexing
     * (e.g. a time window leaderboard needs a window to be created) can override this.
     *
     * @param store the record store to set up
     * @param index the index under test, as built by {@link #buildIndex}
     */
    default void setupIndex(final FDBRecordStore store, final Index index) {
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

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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;

import java.util.List;

/**
 * Describes a single index-under-test for the scenario framework. The framework (via
 * {@link IndexScenarioMetaData} and {@link ScenarioRecords}) owns the record metadata and record
 * generation over the shared {@code TestRecordsIndexScenariosProto} schema, so a definition only
 * has to say <em>what</em> to index (over the standard fields) and <em>how</em> to scan it.
 */
public interface IndexDefinition {
    String getIndexName();

    /**
     * Build the index under test over the standard {@code ScenarioRecord} fields. The framework
     * passes the grouping prefix it wants the index to be grouped by: {@link ScenarioRecords#noPrefix()}
     * (an empty expression) for ungrouped scenarios, or {@code field("group")} for grouped /
     * {@code deleteRecordsWhere} scenarios. The definition composes the prefix into its
     * root/grouping expression (see {@link IndexScenarioMetaData#prefixed}).
     *
     * @param groupingPrefix empty for ungrouped, or the grouping-field expression for grouped scenarios
     * @return the index to add to the store
     */
    Index buildIndex(KeyExpression groupingPrefix);

    /**
     * The record type (or synthetic record type) the index is added to. Normal definitions return
     * {@link ScenarioRecords#SCENARIO_RECORD}.
     *
     * @return the indexed type name
     */
    String getIndexedTypeName();

    RecordCursor<IndexEntry> scanIndex(FDBRecordStore store, ScanProperties scanProperties);

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
     * and {@code SyntheticUnnestedType} scenarios). Default {@code false}; a definition overrides to
     * {@code true} and implements {@link #buildSyntheticIndex} when it supports synthetic types.
     *
     * @return whether the synthetic-type scenarios apply
     */
    default boolean supportsSynthetic() {
        return false;
    }

    /**
     * Build the index over a synthetic record type's constituent field. Only called when
     * {@link #supportsSynthetic()} is {@code true}.
     *
     * @param constituentName the correlation name of the constituent to index
     * @param valueFieldName the field within that constituent to index
     * @return the index to add to the synthetic type
     */
    default Index buildSyntheticIndex(String constituentName, String valueFieldName) {
        throw new UnsupportedOperationException("index does not support synthetic types: " + getIndexName());
    }

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

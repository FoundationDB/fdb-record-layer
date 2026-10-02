/*
 * DeleteWhereGroup.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.expressions.Query;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assumptions;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Saves records spread across several groups, deletes one whole group with
 * {@code deleteRecordsWhere(field("group").equalsValue(g))}, and verifies that the deleted group's
 * entries are gone while the other groups' entries are unchanged. The index is built grouped by the
 * framework's {@code group} field with primary keys aligned to that prefix so the delete can clear
 * whole ranges.
 */
@AutoService(IndexScenario.class)
public class DeleteWhereGroup implements IndexScenario {
    private static final long GROUP_TO_DELETE = 0L;

    @Override
    public void runTest(final IndexDefinitionFactory definitionFactory,
                        final Supplier<FDBRecordContext> openContext,
                        final FDBRecordStore.Builder storeBuilder) {
        final IndexDefinition definition = definitionFactory.getDefinition();
        Assumptions.assumeTrue(definition.supportsGrouping(),
                "index does not support grouped deleteRecordsWhere");
        final IndexScenarioModel model = IndexScenarioModel.grouped(definition, openContext, storeBuilder);
        final List<Message> records = model.generateRecords(12);

        model.setupIndex();
        model.saveRecords(records);

        final List<IndexEntry> before = model.scanIndex();
        model.runAgainstStore(store ->
                store.deleteRecordsWhere(Query.field(ScenarioRecords.GROUP).equalsValue(GROUP_TO_DELETE)));

        final List<IndexEntry> after = model.scanIndex();
        // The grouping field is the first column of every grouped index entry key.
        assertTrue(after.stream().noneMatch(entry -> entry.getKey().getLong(0) == GROUP_TO_DELETE),
                () -> "entries for the deleted group remain: " + after);
        // Everything outside the deleted group must be untouched.
        final List<IndexEntry> expected = before.stream()
                .filter(entry -> entry.getKey().getLong(0) != GROUP_TO_DELETE)
                .collect(Collectors.toList());
        model.assertScanResultsEqual(expected, after);
    }
}

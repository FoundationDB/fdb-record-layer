/*
 * package-info.java
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

/**
 * A framework for testing {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer}s
 * against a shared set of scenarios.
 * <p>
 * Maintainer behaviour is easy to get subtly wrong and hard to cover uniformly. So rather than each
 * index type having its own tests for rebuilding, write-only maintenance, group deletion, synthetic
 * record types and so on, each index type is described once by an {@link
 * com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition}, each scenario
 * once by an {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario},
 * and every scenario runs against every index type. A new index type is tested by all the existing
 * scenarios, and a new scenario runs against all the existing index types, with no wiring between them.
 *
 * <h2>Testing an index type</h2>
 * A test class runs every scenario against its index type with a parameterized test, using the
 * {@link com.apple.foundationdb.record.provider.foundationdb.indexes.IndexScenarios} annotation to
 * supply the scenarios:
 * <pre>
 * &#64;ParameterizedTest
 * &#64;IndexScenarios
 * void indexScenariosTest(IndexScenario scenario) throws Exception {
 *     scenario.runTest(
 *             // the index type under test
 *             new MyIndexDefinition(),
 *             // how to open a transaction
 *             () -&gt; database.openContext(),
 *             // where the store lives; the scenario supplies the metadata
 *             FDBRecordStore.newBuilder().setKeySpacePath(path));
 * }
 * </pre>
 * The scenario is given a way to open a transaction and a store builder, rather than creating its own,
 * so that it fits into existing test classes, which differ in how they do both. The store builder must
 * not set the metadata.
 * <p>
 * {@code @IndexScenarios} also tags the test with {@link com.apple.test.Tags#IndexScenarios}, so that
 * the whole set can be run on its own; in the Record Layer build, the {@code indexScenarioTest} Gradle
 * task does this. A test that builds its own parameters instead, for example a cartesian product of
 * scenarios (from {@link
 * com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenariosArgumentsProvider#getScenarios()})
 * and index configurations, must add {@code @Tag(Tags.IndexScenarios)} itself.
 *
 * <h2>The shared schema</h2>
 * Every scenario uses the record types in {@link com.apple.foundationdb.record.TestRecordsIndexScenariosProto},
 * rather than a record type per index type. The indexed record type, {@code ScenarioRecord}, carries
 * its indexable content in a generic {@code IndexedMessage} sub-message, which has fields for the kinds
 * of values the different index types need. The framework owns everything else:
 * <ul>
 *     <li>the record metadata ({@link
 *     com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData}),
 *     including the joined and unnested synthetic record types;</li>
 *     <li>the primary key ({@code rec_no}) and the grouping field ({@code group}), which it keeps
 *     aligned so that a grouped index can be cleared with {@code deleteRecordsWhere};</li>
 *     <li>generating the records ({@link
 *     com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords}, which also
 *     holds the field and record type names). A second record type, {@code OtherScenarioRecord}, is
 *     never indexed; it is the other side of the joined type, and lets a scenario write records
 *     without touching the index.</li>
 * </ul>
 * If a new index type needs a kind of value {@code IndexedMessage} does not have, add a field to it.
 *
 * <h2>Writing an index definition</h2>
 * An {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition} says
 * what to index and how to scan it, and nothing about records, storage, or scenarios:
 * <ul>
 *     <li>{@code generateIndexedMessage} fills in the {@code IndexedMessage} fields the index uses,
 *     with deterministic values that are distinct (or ordered) wherever the index type needs them to
 *     be.</li>
 *     <li>{@code buildIndex} builds the index, named
 *     {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords#INDEX_NAME}.
 *     The definition writes its key expression relative to the {@code IndexedMessage}, and the
 *     {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget} it is
 *     given roots that expression wherever the message lives in the current configuration, and
 *     supplies the grouping prefix to put in front of it. That one indirection lets the same
 *     {@code buildIndex} serve ungrouped, grouped, joined and unnested configurations, so every index
 *     type is tested over synthetic record types without any extra work.</li>
 *     <li>{@code scanIndex} scans the whole index.</li>
 * </ul>
 * The remaining methods have defaults that suit most index types. {@code configureStore} and
 * {@code setupIndex} cover per-store settings and state that has to exist before records are indexed,
 * and {@code scanResultsEqual} relaxes the comparison of scan results for index types whose scans
 * cannot be compared exactly. When a maintainer genuinely cannot support something that some scenarios
 * rely on, such as group deletion, the definition turns off the corresponding capability (for
 * example {@code supportsGrouping}), with the reason documented. Those scenarios are then skipped for
 * that index type, rather than being weakened for every index type.
 *
 * <h2>Writing a scenario</h2>
 * An {@link com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenario} is
 * discovered with {@link java.util.ServiceLoader}, so a test class never names it: once registered (for
 * example with {@code @AutoService}), it runs in every test that uses {@code @IndexScenarios}. It
 * works through an {@link
 * com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioModel}, which
 * builds the metadata for the chosen configuration, generates matching records, and runs each store
 * or index operation in its own transaction. A scenario should only assert what holds for every index
 * type, and skip itself when it needs a capability the index type does not have:
 * <pre>
 * &#64;AutoService(IndexScenario.class)
 * public class MyScenario implements IndexScenario {
 *     &#64;Override
 *     public void runTest(IndexDefinition definition, Supplier&lt;FDBRecordContext&gt; openContext,
 *                         FDBRecordStore.Builder storeBuilder) {
 *         Assumptions.assumeTrue(definition.supportsGrouping(), "index does not support grouping");
 *         IndexScenarioModel model = IndexScenarioModel.grouped(definition, openContext, storeBuilder);
 *         model.saveRecords(model.generateRecords(12));
 *         List&lt;IndexEntry&gt; maintained = model.scanIndex();
 *         model.markIndexDisabled();
 *         model.buildIndex();
 *         model.assertScanResultsEqual(maintained, model.scanIndex());
 *     }
 * }
 * </pre>
 * Adding an index type or a scenario does not require changing the shared schema (unless
 * {@code IndexedMessage} is missing a kind of value), the other index types, or the framework itself.
 */
package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

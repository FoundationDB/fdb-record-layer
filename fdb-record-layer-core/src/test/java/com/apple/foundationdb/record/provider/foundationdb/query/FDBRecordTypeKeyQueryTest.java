/*
 * FDBRecordTypeKeyQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;


import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.RecordTypesProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicates;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.reverse;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValueWithFieldNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
public class FDBRecordTypeKeyQueryTest extends FDBRecordStoreQueryTestBase {
    private static final boolean[] BOOLEANS = new boolean[]{false, true};

    private void openStore(@Nonnull FDBRecordContext context) {
        RecordMetaDataHook hook = metaDataBuilder -> {
            RecordTypeBuilder simpleBuilder = metaDataBuilder.getRecordType("MySimpleRecord");
            simpleBuilder.setPrimaryKey(concat(recordType(), field("rec_no")));

            RecordTypeBuilder otherBuilder = metaDataBuilder.getRecordType("MyOtherRecord");
            otherBuilder.setPrimaryKey(concat(recordType(), field("num_value_2"), field("rec_no")));

            Index universalIndexWithRecordTypeColumn = new Index("record_type+num_value_2", concat(recordType(), field("num_value_2")));
            metaDataBuilder.addUniversalIndex(universalIndexWithRecordTypeColumn);

            metaDataBuilder.removeIndex("MySimpleRecord$num_value_3_indexed");
            Index singleTypeIndexWithRecordTypeColumn = new Index("MySimpleRecord$record_type+num_value_3_indexed", concat(recordType(), field("num_value_3_indexed")));
            metaDataBuilder.addIndex(simpleBuilder, singleTypeIndexWithRecordTypeColumn);
        };
        openSimpleRecordStore(context, hook);

        assertTrue(recordStore.getRecordMetaData().primaryKeyHasRecordTypePrefix());
        assertNull(recordStore.getRecordMetaData().commonPrimaryKey());
    }

    @DualPlannerTest
    void scanByType() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            for (String typeName : recordStore.getRecordMetaData().getRecordTypes().keySet()) {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType(typeName)
                        .build();
                RecordQueryPlan plan = planner.plan(query);

                assertMatchesExactly(plan, scanPlan()
                        .where(scanComparisons(range("[IS " + typeName + "]")))
                );
                assertEquals(Set.of(typeName), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void orderByPrimaryKey() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            for (RecordType type : recordStore.getRecordMetaData().getRecordTypes().values()) {
                for (boolean reverse : BOOLEANS) {
                    RecordQuery query = RecordQuery.newBuilder()
                            .setRecordType(type.getName())
                            // Remove the record type key from the sort criteria
                            .setSort(type.getPrimaryKey().getSubKey(1, type.getPrimaryKey().getColumnSize()), reverse)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);

                    assertMatchesExactly(plan, scanPlan()
                            .where(scanComparisons(range("[IS " + type.getName() + "]")))
                            .and(reverse(reverse))
                    );
                    assertEquals(Set.of(type.getName()), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
                }
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sortByPrimaryKeyIncludingRecordTypeKey() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            for (RecordType type : recordStore.getRecordMetaData().getRecordTypes().values()) {
                for (boolean reverse : BOOLEANS) {
                    RecordQuery query = RecordQuery.newBuilder()
                            .setRecordType(type.getName())
                            .setSort(type.getPrimaryKey(), reverse)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);

                    if (reverse) {
                        // FIXME: Ordering doesn't match equaility-bound reverse sorts correctly
                        // Otherwise, this should be planned identically to the forward order direction
                        assertMatchesExactly(plan, typeFilterPlan(
                                scanPlan()
                                        .where(scanComparisons(unbounded()))
                                        .and(reverse(true))
                                ).where(recordTypes(exactly(PrimitiveMatchers.equalsObject(type.getName()))))
                        );
                    } else {
                        assertMatchesExactly(plan, scanPlan()
                                .where(scanComparisons(range("[IS " + type.getName() + "]")))
                                .and(reverse(false))
                        );
                    }
                    assertEquals(Set.of(type.getName()), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
                }
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void sharedNumValue2Index() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            for (RecordType type : recordStore.getRecordMetaData().getRecordTypes().values()) {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType(type.getName())
                        .setFilter(Query.field("num_value_2").equalsValue(5))
                        .build();
                RecordQueryPlan plan = planner.plan(query);

                if (type.getName().equals("MySimpleRecord")) {
                    // Some day, should prefer to use the shared index:
                    // assertMatchesExactly(plan, indexPlan()
                    //         .where(indexName("record_type+num_value_2"))
                    //         .and(scanComparisons(range("[IS " + type.getName() + ", EQUALS 5]")))
                    // );
                    assertMatchesExactly(plan, RecordQueryPlanMatchers.predicatesFilterPlan(
                            scanPlan()
                                    .where(scanComparisons(range("[IS MySimpleRecord]")))
                            )
                            .where(predicates(exactly(List.of(valuePredicate(fieldValueWithFieldNames("num_value_2"), new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5))))))
                    );
                } else {
                    assertMatchesExactly(plan, scanPlan()
                            .where(scanComparisons(range("[IS MyOtherRecord, EQUALS 5]")))
                    );
                }

                assertEquals(Set.of(type.getName()), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void singleTypeNumValue3Index() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_3_indexed").equalsValue(42))
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertMatchesExactly(plan, indexPlan()
                    .where(indexName("MySimpleRecord$record_type+num_value_3_indexed"))
                    .and(scanComparisons(range("[IS MySimpleRecord, EQUALS 42]")))
            );

            assertEquals(Set.of("MySimpleRecord"), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void singleTypeOrderByNumValue3Index() {
        try (FDBRecordContext context = openContext()) {
            openStore(context);

            for (boolean reverse : BOOLEANS) {
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("MySimpleRecord")
                        .setSort(field("num_value_3_indexed"), reverse)
                        .build();
                RecordQueryPlan plan = planner.plan(query);

                assertMatchesExactly(plan, indexPlan()
                        .where(indexName("MySimpleRecord$record_type+num_value_3_indexed"))
                        .and(scanComparisons(range("[IS MySimpleRecord]")))
                        .and(reverse(reverse))
                );
                assertEquals(Set.of("MySimpleRecord"), RecordTypesProperty.evaluate(GroupExpressionRef.of(plan)));
            }
        }
    }
}

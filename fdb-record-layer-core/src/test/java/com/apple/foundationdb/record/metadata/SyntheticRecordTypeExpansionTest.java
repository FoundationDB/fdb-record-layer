/*
 * SyntheticRecordTypeExpansionTest.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TestRecordsNestedChainProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PrimaryAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of {@link UnnestedRecordType#expand(AccessHint)}, of {@link JoinedRecordType#expand(AccessHint)}, and of the
 * planner types the expansion is built from. All of this is metadata-to-graph work, so none of these tests needs a
 * record store.
 */
class SyntheticRecordTypeExpansionTest {
    @Nonnull
    private static final String OUTER = "OuterRecord";
    @Nonnull
    private static final String PARENT = "parent";
    @Nonnull
    private static final String UNNESTED_MAP = "UnnestedMap";
    @Nonnull
    private static final String TWO_UNNESTED_MAPS = "TwoUnnestedMaps";
    @Nonnull
    private static final String NESTED_CHAIN = "NestedChain";
    @Nonnull
    private static final String UNNESTED_REVIEWS = "UnnestedReviews";
    @Nonnull
    private static final String ESCAPED_NAMES = "EscapedNames";
    @Nonnull
    private static final String JOINED = "Joined";
    @Nonnull
    private static final Descriptors.Descriptor INNER_DESCRIPTOR =
            TestRecordsNestedChainProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor();
    @Nonnull
    private static final KeyExpression ENTRIES_FAN_OUT = field("map").nest(field("entry", FanType.FanOut));
    @Nonnull
    private static final AccessHint ACCESS_HINT = new PrimaryAccessHint();

    @Test
    void expandUnnestsAConstituentWithOrdinality() {
        final RecordMetaData metaData = mapMetaData(metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED_MAP);
            typeBuilder.addParentConstituent(PARENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("map_entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT, ENTRIES_FAN_OUT);
        });
        final UnnestedRecordType type = unnestedType(metaData, UNNESTED_MAP);

        final GraphExpansion expansion = type.expand(ACCESS_HINT);
        assertThat(columnNames(expansion), contains(PARENT, "map_entry", UnnestedRecordType.POSITIONS_FIELD));
        assertEquals(2, expansion.getQuantifiers().size());

        // The parent constituent is a scan of the stored type, restricted to it and typed as the planner sees it.
        final Quantifier parentQuantifier = expansion.getQuantifiers().get(0);
        final LogicalTypeFilterExpression typeFilter = assertTypeFilter(parentQuantifier, OUTER);
        assertEquals(metaData.getPlannerType(OUTER), typeFilter.getResultValue().getResultType());
        final FullUnorderedScanExpression scan =
                (FullUnorderedScanExpression)Iterables.getOnlyElement(typeFilter.getQuantifiers()).getRangesOver().get();
        assertEquals(metaData.getRecordTypes().keySet(), scan.getRecordTypes());
        assertThat(scan.getAccessHints().getAccessHintSet(), contains(ACCESS_HINT));
        assertEquals(parentQuantifier.getFlowedObjectValue(), columnValue(expansion, PARENT));

        // The nested constituent explodes `parent.map.entry`, with ordinality so that positions can be recovered.
        final Quantifier entryQuantifier = expansion.getQuantifiers().get(1);
        final ExplodeExpression explode = assertSelectOverExplode(entryQuantifier);
        assertTrue(explode.isWithOrdinality());
        assertEquals(FieldValue.ofFieldNames(parentQuantifier.getFlowedObjectValue(), List.of("map", "entry")),
                explode.getCollectionValue());

        // The constituent's column is the element the explode flows, not the `(element, ordinal)` pair, and it is typed
        // as the synthetic record type declares that constituent.
        assertEquals(FieldValue.ofOrdinalNumber(entryQuantifier.getFlowedObjectValue(), 0),
                columnValue(expansion, "map_entry"));
        assertEquals(Type.Record.fromDescriptor(type.getDescriptor()).getFieldNameFieldMap().get("map_entry").getFieldType(),
                columnValue(expansion, "map_entry").getResultType());
    }

    @Test
    void expandFlowsOnePositionPerNestedConstituent() {
        final RecordMetaData metaData = mapMetaData(addTwoMapsType());
        final UnnestedRecordType type = unnestedType(metaData, TWO_UNNESTED_MAPS);

        final GraphExpansion expansion = type.expand(ACCESS_HINT);
        assertThat(columnNames(expansion),
                contains(PARENT, "entry_one", "entry_two", UnnestedRecordType.POSITIONS_FIELD));
        assertEquals(3, expansion.getQuantifiers().size());

        // The positions record has a field per nested constituent, and no field for the parent, which has no position.
        final Value positionsValue = columnValue(expansion, UnnestedRecordType.POSITIONS_FIELD);
        assertEquals(Type.Record.fromFields(false,
                        ImmutableList.of(longField("entry_one"), longField("entry_two"))),
                positionsValue.getResultType());

        // Each position is the ordinal its own explode flows, widened to the `LONG` the positions field declares.
        for (int i = 0; i < 2; i++) {
            final Quantifier constituentQuantifier = expansion.getQuantifiers().get(i + 1);
            final Value positionValue = Iterables.get(positionsValue.getChildren(), i);
            assertEquals(constituentQuantifier.getAlias(),
                    Iterables.getOnlyElement(positionValue.getCorrelatedTo()));
            assertEquals(FieldValue.ofOrdinalNumber(constituentQuantifier.getFlowedObjectValue(), 1),
                    Iterables.getOnlyElement(positionValue.getChildren()));
        }

        // Both constituents unnest the same array, but each gets its own explode, so that they range independently.
        final ExplodeExpression explodeOne = assertSelectOverExplode(expansion.getQuantifiers().get(1));
        final ExplodeExpression explodeTwo = assertSelectOverExplode(expansion.getQuantifiers().get(2));
        assertNotEquals(expansion.getQuantifiers().get(1).getAlias(), expansion.getQuantifiers().get(2).getAlias());
        assertEquals(explodeOne.getCollectionValue(), explodeTwo.getCollectionValue());
    }

    @Test
    void expandChainsNestedConstituentsOntoTheirOwner() {
        final RecordMetaData metaData = nestedChainMetaData(addNestedChainType());
        final UnnestedRecordType type = unnestedType(metaData, NESTED_CHAIN);

        final GraphExpansion expansion = type.expand(ACCESS_HINT);
        assertThat(columnNames(expansion),
                contains(PARENT, "middle", "inner", "outer_inner", UnnestedRecordType.POSITIONS_FIELD));
        assertEquals(4, expansion.getQuantifiers().size());

        final Quantifier parentQuantifier = expansion.getQuantifiers().get(0);
        final Quantifier middleQuantifier = expansion.getQuantifiers().get(1);

        // `middle` and `outer_inner` hang off the stored record, ...
        assertEquals(FieldValue.ofFieldNames(parentQuantifier.getFlowedObjectValue(), List.of("many_middle")),
                assertSelectOverExplode(middleQuantifier).getCollectionValue());
        assertEquals(FieldValue.ofFieldNames(parentQuantifier.getFlowedObjectValue(), List.of("inner")),
                assertSelectOverExplode(expansion.getQuantifiers().get(3)).getCollectionValue());

        // ... while `inner` hangs off the element `middle` flows, which is what makes this a chain rather than a fan.
        final Value innerCollectionValue = assertSelectOverExplode(expansion.getQuantifiers().get(2))
                .getCollectionValue();
        assertEquals(FieldValue.ofFieldNames(
                        FieldValue.ofOrdinalNumber(middleQuantifier.getFlowedObjectValue(), 0), List.of("inner")),
                innerCollectionValue);
        assertEquals(middleQuantifier.getAlias(), Iterables.getOnlyElement(innerCollectionValue.getCorrelatedTo()));

        // Every nested constituent still contributes its own position, chained or not.
        assertEquals(Type.Record.fromFields(false, ImmutableList.of(
                        longField("middle"), longField("inner"), longField("outer_inner"))),
                columnValue(expansion, UnnestedRecordType.POSITIONS_FIELD).getResultType());
    }

    @Test
    void expandUnnestsThroughANullableArrayWrapper() {
        final RecordMetaData metaData = wrapperMetaData(metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED_REVIEWS);
            typeBuilder.addParentConstituent(PARENT, metaDataBuilder.getRecordType("RestaurantRecord"));
            typeBuilder.addNestedConstituent("review", TestRecords4WrapperProto.RestaurantReview.getDescriptor(),
                    PARENT, field("reviews").nest(field("values", FanType.FanOut)));
        });
        final UnnestedRecordType type = unnestedType(metaData, UNNESTED_REVIEWS);

        final GraphExpansion expansion = type.expand(ACCESS_HINT);
        // The planner models a nullable array as an array, not as the wrapper message the records store it in, so the
        // path to the exploded array stops at the wrapper field and must not descend into its repeated field.
        final Value collectionValue = assertSelectOverExplode(expansion.getQuantifiers().get(1)).getCollectionValue();
        assertEquals(FieldValue.ofFieldNames(expansion.getQuantifiers().get(0).getFlowedObjectValue(),
                List.of("reviews")), collectionValue);
        assertEquals(Type.Record.fromDescriptor(type.getDescriptor()).getFieldNameFieldMap().get("review").getFieldType(),
                columnValue(expansion, "review").getResultType());
    }

    @Test
    void expandUnnestsThroughEscapedFieldNames() {
        final RecordMetaData metaData = nestedChainMetaData(metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(ESCAPED_NAMES);
            typeBuilder.addParentConstituent(PARENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("direct", INNER_DESCRIPTOR, PARENT,
                    field("escaped__2inner", FanType.FanOut));
            typeBuilder.addNestedConstituent("nested", INNER_DESCRIPTOR, PARENT,
                    field("nested__2holder").nest(field("inner", FanType.FanOut)));
        });
        final UnnestedRecordType type = unnestedType(metaData, ESCAPED_NAMES);

        final GraphExpansion expansion = type.expand(ACCESS_HINT);
        // A key expression addresses protobuf fields, so it carries the names the descriptor uses; a `FieldValue`
        // resolves against the planner's type, whose field names those have been decoded into. The exploded path is
        // therefore expressed in the decoded names, and a field whose descriptor name carries an escape sequence
        // cannot be found under the name the key expression holds.
        final Value parentValue = expansion.getQuantifiers().get(0).getFlowedObjectValue();
        assertEquals(FieldValue.ofFieldNames(parentValue, List.of("escaped.inner")),
                assertSelectOverExplode(expansion.getQuantifiers().get(1)).getCollectionValue());
        assertEquals(FieldValue.ofFieldNames(parentValue, List.of("nested.holder", "inner")),
                assertSelectOverExplode(expansion.getQuantifiers().get(2)).getCollectionValue());
    }

    @Test
    void expandScansEachJoinedConstituentAndJoinsThemOnEquality() {
        final RecordMetaData metaData = mapMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("outer", OUTER);
            typeBuilder.addConstituent("other", "OtherRecord");
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion), contains("outer", "other"));
        assertEquals(2, expansion.getQuantifiers().size());

        // Each constituent is a scan of its stored type, flowed whole as the constituent's column and typed as the
        // planner sees that stored type.
        final Quantifier outerQuantifier = expansion.getQuantifiers().get(0);
        final Quantifier otherQuantifier = expansion.getQuantifiers().get(1);
        final LogicalTypeFilterExpression outerTypeFilter = assertTypeFilter(outerQuantifier, OUTER);
        assertTypeFilter(otherQuantifier, "OtherRecord");
        assertEquals(metaData.getPlannerType(OUTER), outerTypeFilter.getResultValue().getResultType());
        final FullUnorderedScanExpression scan = (FullUnorderedScanExpression)Iterables.getOnlyElement(
                outerTypeFilter.getQuantifiers()).getRangesOver().get();
        assertThat(scan.getAccessHints().getAccessHintSet(), contains(ACCESS_HINT));
        assertEquals(outerQuantifier.getFlowedObjectValue(), columnValue(expansion, "outer"));
        assertEquals(otherQuantifier.getFlowedObjectValue(), columnValue(expansion, "other"));

        // The join is an equality between a column of each constituent.
        assertThat(expansion.getPredicates(), contains(
                equality(FieldValue.ofFieldName(outerQuantifier.getFlowedObjectValue(), "other_id"),
                        FieldValue.ofFieldName(otherQuantifier.getFlowedObjectValue(), "other_id"))));
    }

    @Test
    void expandScansASelfJoinedTypeOncePerConstituent() {
        final RecordMetaData metaData = joinIndexMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple1", "MySimpleRecord");
            typeBuilder.addConstituent("simple2", "MySimpleRecord");
            typeBuilder.addJoin("simple1", "other_rec_no", "simple2", "rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion), contains("simple1", "simple2"));

        // Both copies scan the same stored type, but each through its own quantifier, so that they range
        // independently and the join can relate one copy to the other.
        final Quantifier firstQuantifier = expansion.getQuantifiers().get(0);
        final Quantifier secondQuantifier = expansion.getQuantifiers().get(1);
        assertTypeFilter(firstQuantifier, "MySimpleRecord");
        assertTypeFilter(secondQuantifier, "MySimpleRecord");
        assertNotEquals(firstQuantifier.getAlias(), secondQuantifier.getAlias());
        assertThat(expansion.getPredicates(), contains(
                equality(FieldValue.ofFieldName(firstQuantifier.getFlowedObjectValue(), "other_rec_no"),
                        FieldValue.ofFieldName(secondQuantifier.getFlowedObjectValue(), "rec_no"))));
    }

    @Test
    void expandAddsOneEqualityPerJoin() {
        final RecordMetaData metaData = joinIndexMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("a", "TypeA");
            typeBuilder.addConstituent("b", "TypeB");
            typeBuilder.addConstituent("c", "TypeC");
            typeBuilder.addJoin("a", "type_b_rec_no", "b", "rec_no");
            typeBuilder.addJoin("b", "type_c_rec_no", "c", "rec_no");
            typeBuilder.addJoin("c", "type_a_rec_no", "a", "rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion), contains("a", "b", "c"));
        final Value a = expansion.getQuantifiers().get(0).getFlowedObjectValue();
        final Value b = expansion.getQuantifiers().get(1).getFlowedObjectValue();
        final Value c = expansion.getQuantifiers().get(2).getFlowedObjectValue();

        // Every join is kept, including the one closing the cycle, even though it is not needed to connect the three.
        assertThat(expansion.getPredicates(), contains(
                equality(FieldValue.ofFieldName(a, "type_b_rec_no"), FieldValue.ofFieldName(b, "rec_no")),
                equality(FieldValue.ofFieldName(b, "type_c_rec_no"), FieldValue.ofFieldName(c, "rec_no")),
                equality(FieldValue.ofFieldName(c, "type_a_rec_no"), FieldValue.ofFieldName(a, "rec_no"))));
    }

    @Test
    void expandReadsEachSideOfAJoinFromItsOwnConstituent() {
        final RecordMetaData metaData = joinIndexMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple", "MySimpleRecord");
            typeBuilder.addConstituent("other", "MyOtherRecord");
            // The join names the constituents in the opposite order from the one they were added in.
            typeBuilder.addJoin("other", "rec_no", "simple", "other_rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        // The columns follow the constituents, ...
        assertThat(columnNames(expansion), contains("simple", "other"));
        // ... while the equality follows the join.
        assertThat(expansion.getPredicates(), contains(
                equality(FieldValue.ofFieldName(expansion.getQuantifiers().get(1).getFlowedObjectValue(), "rec_no"),
                        FieldValue.ofFieldName(expansion.getQuantifiers().get(0).getFlowedObjectValue(),
                                "other_rec_no"))));
    }

    @Test
    void expandJoinsOnANestedField() {
        final RecordMetaData metaData = joinIndexMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("customer", "CustomerWithHeader");
            typeBuilder.addConstituent("order", "OrderWithHeader");
            typeBuilder.addJoin("order", field("custRef").nest("string_value"), "customer", field("name"));
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        final Value customer = expansion.getQuantifiers().get(0).getFlowedObjectValue();
        final Value order = expansion.getQuantifiers().get(1).getFlowedObjectValue();
        assertThat(expansion.getPredicates(), contains(
                equality(FieldValue.ofFieldNames(order, List.of("custRef", "string_value")),
                        FieldValue.ofFieldName(customer, "name"))));
    }

    @Test
    void expandWithoutJoinsHasNoPredicates() {
        final RecordMetaData metaData = joinIndexMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple", "MySimpleRecord");
            typeBuilder.addConstituent("other", "MyOtherRecord");
        });

        // Without a join, every pair of constituent records is a joined record.
        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion), contains("simple", "other"));
        assertEquals(2, expansion.getQuantifiers().size());
        assertThat(expansion.getPredicates(), empty());
    }

    @Test
    void expandOnAnOuterJoinedConstituentIsUnsupported() {
        final RecordMetaData metaData = mapMetaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("outer", metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addConstituent("other", metaDataBuilder.getRecordType("OtherRecord"), true);
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
        });

        // An outer join keeps a record without a partner, which an equality between the two scans would drop.
        final JoinedRecordType type = joinedType(metaData);
        final UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, () -> type.expand(ACCESS_HINT));
        assertEquals("cannot expand an outer-joined constituent of a JoinedRecordType", exception.getMessage());
    }

    @Nonnull
    private static RecordMetaData mapMetaData(@Nonnull Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData nestedChainMetaData(@Nonnull Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedChainProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData wrapperMetaData(@Nonnull Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecords4WrapperProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaData joinIndexMetaData(@Nonnull Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static Consumer<RecordMetaDataBuilder> addTwoMapsType() {
        return metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(TWO_UNNESTED_MAPS);
            typeBuilder.addParentConstituent(PARENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("entry_one", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT, ENTRIES_FAN_OUT);
            typeBuilder.addNestedConstituent("entry_two", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    PARENT, ENTRIES_FAN_OUT);
        };
    }

    @Nonnull
    private static Consumer<RecordMetaDataBuilder> addNestedChainType() {
        return metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType(NESTED_CHAIN);
            typeBuilder.addParentConstituent(PARENT, metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("middle",
                    TestRecordsNestedChainProto.OuterRecord.MiddleRecord.getDescriptor(),
                    PARENT, field("many_middle", FanType.FanOut));
            typeBuilder.addNestedConstituent("inner",
                    TestRecordsNestedChainProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    "middle", field("inner", FanType.FanOut));
            typeBuilder.addNestedConstituent("outer_inner",
                    TestRecordsNestedChainProto.OuterRecord.MiddleRecord.InnerRecord.getDescriptor(),
                    PARENT, field("inner", FanType.FanOut));
        };
    }

    @Nonnull
    private static Type.Record.Field longField(@Nonnull String fieldName) {
        return Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of(fieldName));
    }

    @Nonnull
    private static UnnestedRecordType unnestedType(@Nonnull RecordMetaData metaData, @Nonnull String typeName) {
        final SyntheticRecordType<?> syntheticRecordType = metaData.getSyntheticRecordType(typeName);
        assertThat(syntheticRecordType, instanceOf(UnnestedRecordType.class));
        return (UnnestedRecordType)syntheticRecordType;
    }

    @Nonnull
    private static JoinedRecordType joinedType(@Nonnull RecordMetaData metaData) {
        final SyntheticRecordType<?> syntheticRecordType = metaData.getSyntheticRecordType(JOINED);
        assertThat(syntheticRecordType, instanceOf(JoinedRecordType.class));
        return (JoinedRecordType)syntheticRecordType;
    }

    @Nonnull
    private static ValuePredicate equality(@Nonnull Value left, @Nonnull Value right) {
        return new ValuePredicate(left, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, right));
    }

    @Nonnull
    private static List<String> columnNames(@Nonnull GraphExpansion expansion) {
        return expansion.getResultColumns().stream()
                .map(column -> column.getField().getFieldName())
                .collect(Collectors.toList());
    }

    @Nonnull
    private static Value columnValue(@Nonnull GraphExpansion expansion, @Nonnull String columnName) {
        return expansion.getResultColumns().stream()
                .filter(column -> columnName.equals(column.getField().getFieldNameOptional().orElse(null)))
                .map(Column::getValue)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column named " + columnName));
    }

    @Nonnull
    private static LogicalTypeFilterExpression assertTypeFilter(@Nonnull Quantifier quantifier,
                                                                @Nonnull String recordTypeName) {
        final RelationalExpression expression = quantifier.getRangesOver().get();
        assertThat(expression, instanceOf(LogicalTypeFilterExpression.class));
        final LogicalTypeFilterExpression typeFilter = (LogicalTypeFilterExpression)expression;
        assertThat(typeFilter.getRecordTypes(), contains(recordTypeName));
        assertThat(Iterables.getOnlyElement(typeFilter.getQuantifiers()).getRangesOver().get(),
                instanceOf(FullUnorderedScanExpression.class));
        return typeFilter;
    }

    /**
     * Asserts that the given quantifier ranges over a select that returns the exploded struct itself, and returns the
     * explode underneath it.
     */
    @Nonnull
    private static ExplodeExpression assertSelectOverExplode(@Nonnull Quantifier quantifier) {
        final RelationalExpression expression = quantifier.getRangesOver().get();
        assertThat(expression, instanceOf(SelectExpression.class));
        final SelectExpression select = (SelectExpression)expression;
        final Quantifier explodeQuantifier = Iterables.getOnlyElement(select.getQuantifiers());
        assertEquals(explodeQuantifier.getFlowedObjectValue(), select.getResultValue());
        final RelationalExpression explode = explodeQuantifier.getRangesOver().get();
        assertThat(explode, instanceOf(ExplodeExpression.class));
        return (ExplodeExpression)explode;
    }
}

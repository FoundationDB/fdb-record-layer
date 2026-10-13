/*
 * JoinedRecordTypeExpansionTest.java
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
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PrimaryAccessHint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link JoinedRecordType#expand(AccessHint)}. All of this is metadata-to-graph work, so none of these tests
 * needs a record store.
 */
class JoinedRecordTypeExpansionTest {
    @Nonnull
    private static final String JOINED = "Joined";
    @Nonnull
    private static final AccessHint ACCESS_HINT = new PrimaryAccessHint();

    @Test
    void expandScansEachConstituentAndJoinsThemOnEquality() {
        final RecordMetaData metaData = metaData(TestRecordsNestedMapProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("outer", "OuterRecord");
            typeBuilder.addConstituent("other", "OtherRecord");
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion)).containsExactly("outer", "other");
        assertThat(expansion.getQuantifiers()).hasSize(2);

        // Each constituent is a scan of its stored type, flowed whole as the constituent's column and typed as the
        // planner sees that stored type.
        final Quantifier outerQuantifier = expansion.getQuantifiers().get(0);
        final Quantifier otherQuantifier = expansion.getQuantifiers().get(1);
        final LogicalTypeFilterExpression outerTypeFilter = assertTypeFilter(outerQuantifier, "OuterRecord");
        assertTypeFilter(otherQuantifier, "OtherRecord");
        assertThat(outerTypeFilter.getResultValue().getResultType()).isEqualTo(metaData.getPlannerType("OuterRecord"));
        final FullUnorderedScanExpression scan = (FullUnorderedScanExpression)Iterables.getOnlyElement(
                outerTypeFilter.getQuantifiers()).getRangesOver().get();
        assertThat(scan.getAccessHints().getAccessHintSet()).containsExactly(ACCESS_HINT);
        assertThat(columnValue(expansion, "outer")).isEqualTo(outerQuantifier.getFlowedObjectValue());
        assertThat(columnValue(expansion, "other")).isEqualTo(otherQuantifier.getFlowedObjectValue());

        // The join is an equality between a column of each constituent.
        assertThat(expansion.getPredicates()).containsExactly(
                equality(FieldValue.ofFieldName(outerQuantifier.getFlowedObjectValue(), "other_id"),
                        FieldValue.ofFieldName(otherQuantifier.getFlowedObjectValue(), "other_id")));
    }

    @Test
    void expandScansASelfJoinedTypeOncePerConstituent() {
        final RecordMetaData metaData = metaData(TestRecordsJoinIndexProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple1", "MySimpleRecord");
            typeBuilder.addConstituent("simple2", "MySimpleRecord");
            typeBuilder.addJoin("simple1", "other_rec_no", "simple2", "rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion)).containsExactly("simple1", "simple2");

        // Both copies scan the same stored type, but each through its own quantifier, so that they range
        // independently and the join can relate one copy to the other.
        final Quantifier firstQuantifier = expansion.getQuantifiers().get(0);
        final Quantifier secondQuantifier = expansion.getQuantifiers().get(1);
        assertTypeFilter(firstQuantifier, "MySimpleRecord");
        assertTypeFilter(secondQuantifier, "MySimpleRecord");
        assertThat(firstQuantifier.getAlias()).isNotEqualTo(secondQuantifier.getAlias());
        assertThat(expansion.getPredicates()).containsExactly(
                equality(FieldValue.ofFieldName(firstQuantifier.getFlowedObjectValue(), "other_rec_no"),
                        FieldValue.ofFieldName(secondQuantifier.getFlowedObjectValue(), "rec_no")));
    }

    @Test
    void expandAddsOneEqualityPerJoin() {
        final RecordMetaData metaData = metaData(TestRecordsJoinIndexProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("a", "TypeA");
            typeBuilder.addConstituent("b", "TypeB");
            typeBuilder.addConstituent("c", "TypeC");
            typeBuilder.addJoin("a", "type_b_rec_no", "b", "rec_no");
            typeBuilder.addJoin("b", "type_c_rec_no", "c", "rec_no");
            typeBuilder.addJoin("c", "type_a_rec_no", "a", "rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion)).containsExactly("a", "b", "c");
        final Value a = expansion.getQuantifiers().get(0).getFlowedObjectValue();
        final Value b = expansion.getQuantifiers().get(1).getFlowedObjectValue();
        final Value c = expansion.getQuantifiers().get(2).getFlowedObjectValue();

        // Every join is kept, including the one closing the cycle, even though it is not needed to connect the three.
        assertThat(expansion.getPredicates()).containsExactly(
                equality(FieldValue.ofFieldName(a, "type_b_rec_no"), FieldValue.ofFieldName(b, "rec_no")),
                equality(FieldValue.ofFieldName(b, "type_c_rec_no"), FieldValue.ofFieldName(c, "rec_no")),
                equality(FieldValue.ofFieldName(c, "type_a_rec_no"), FieldValue.ofFieldName(a, "rec_no")));
    }

    @Test
    void expandReadsEachSideOfAJoinFromItsOwnConstituent() {
        final RecordMetaData metaData = metaData(TestRecordsJoinIndexProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple", "MySimpleRecord");
            typeBuilder.addConstituent("other", "MyOtherRecord");
            // The join names the constituents in the opposite order from the one they were added in.
            typeBuilder.addJoin("other", "rec_no", "simple", "other_rec_no");
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        // The columns follow the constituents, ...
        assertThat(columnNames(expansion)).containsExactly("simple", "other");
        // ... while the equality follows the join.
        assertThat(expansion.getPredicates()).containsExactly(
                equality(FieldValue.ofFieldName(expansion.getQuantifiers().get(1).getFlowedObjectValue(), "rec_no"),
                        FieldValue.ofFieldName(expansion.getQuantifiers().get(0).getFlowedObjectValue(),
                                "other_rec_no")));
    }

    @Test
    void expandJoinsOnANestedField() {
        final RecordMetaData metaData = metaData(TestRecordsJoinIndexProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("customer", "CustomerWithHeader");
            typeBuilder.addConstituent("order", "OrderWithHeader");
            typeBuilder.addJoin("order", field("custRef").nest("string_value"), "customer", field("name"));
        });

        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        final Value customer = expansion.getQuantifiers().get(0).getFlowedObjectValue();
        final Value order = expansion.getQuantifiers().get(1).getFlowedObjectValue();
        assertThat(expansion.getPredicates()).containsExactly(
                equality(FieldValue.ofFieldNames(order, List.of("custRef", "string_value")),
                        FieldValue.ofFieldName(customer, "name")));
    }

    @Test
    void expandWithoutJoinsHasNoPredicates() {
        final RecordMetaData metaData = metaData(TestRecordsJoinIndexProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("simple", "MySimpleRecord");
            typeBuilder.addConstituent("other", "MyOtherRecord");
        });

        // Without a join, every pair of constituent records is a joined record.
        final GraphExpansion expansion = joinedType(metaData).expand(ACCESS_HINT);
        assertThat(columnNames(expansion)).containsExactly("simple", "other");
        assertThat(expansion.getQuantifiers()).hasSize(2);
        assertThat(expansion.getPredicates()).isEmpty();
    }

    @Test
    void expandOnAnOuterJoinedConstituentIsUnsupported() {
        final RecordMetaData metaData = metaData(TestRecordsNestedMapProto.getDescriptor(), metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("outer", metaDataBuilder.getRecordType("OuterRecord"));
            typeBuilder.addConstituent("other", metaDataBuilder.getRecordType("OtherRecord"), true);
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
        });

        // An outer join keeps a record without a partner, which an equality between the two scans would drop.
        final JoinedRecordType type = joinedType(metaData);
        assertThatThrownBy(() -> type.expand(ACCESS_HINT))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("cannot expand an outer-joined constituent of a JoinedRecordType");
    }

    @Nonnull
    private static RecordMetaData metaData(@Nonnull Descriptors.FileDescriptor records,
                                           @Nonnull Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(records);
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static JoinedRecordType joinedType(@Nonnull RecordMetaData metaData) {
        final SyntheticRecordType<?> syntheticRecordType = metaData.getSyntheticRecordType(JOINED);
        assertThat(syntheticRecordType).isInstanceOf(JoinedRecordType.class);
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
        assertThat(expression).isInstanceOf(LogicalTypeFilterExpression.class);
        final LogicalTypeFilterExpression typeFilter = (LogicalTypeFilterExpression)expression;
        assertThat(typeFilter.getRecordTypes()).containsExactly(recordTypeName);
        assertThat(Iterables.getOnlyElement(typeFilter.getQuantifiers()).getRangesOver().get())
                .isInstanceOf(FullUnorderedScanExpression.class);
        return typeFilter;
    }
}

/*
 * RecordMetaDataPlannerTypeTest.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the planner types {@link RecordMetaData} describes record types with, by name and by {@link
 * com.apple.foundationdb.record.metadata.RecordType}.
 */
class RecordMetaDataPlannerTypeTest {
    @Nonnull
    private static final String OUTER = "OuterRecord";
    @Nonnull
    private static final String OTHER = "OtherRecord";
    @Nonnull
    private static final String JOINED = "Joined";

    @Test
    void getPlannerTypeForRecordTypeDescribesASyntheticType() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED);
            typeBuilder.addConstituent("outer", OUTER);
            typeBuilder.addConstituent("other", OTHER);
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
        });
        final SyntheticRecordType<?> type = metaData.getSyntheticRecordType(JOINED);

        // A synthetic type's name cannot be resolved against the stored types, so only the type-taking overload can
        // describe it.
        assertThatThrownBy(() -> metaData.getPlannerType(JOINED)).isInstanceOf(MetaDataException.class);
        final Type.Record plannerType = metaData.getPlannerTypeForRecordType(type);
        assertThat(plannerType).isEqualTo(Type.Record.fromDescriptor(type.getDescriptor()));
        assertThat(fieldNames(plannerType)).containsExactly("outer", "other");
        assertThat(metaData.getPlannerTypeForRecordTypes(List.of(type))).isEqualTo(plannerType);
    }

    @Test
    void getPlannerTypeForRecordTypeMatchesTheNameTakingOverloads() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(true));

        assertThat(metaData.getPlannerTypeForRecordType(metaData.getRecordType(OUTER)))
                .isEqualTo(metaData.getPlannerType(OUTER));
        assertThat(metaData.getPlannerTypeForRecordTypes(List.of(metaData.getRecordType(OUTER))))
                .isEqualTo(metaData.getPlannerType(List.of(OUTER)));
        assertThat(metaData.getPlannerTypeForRecordTypes(
                        List.of(metaData.getRecordType(OUTER), metaData.getRecordType(OTHER))))
                .isEqualTo(metaData.getPlannerType(List.of(OUTER, OTHER)));

        // Storing record versions adds the pseudo field to every stored type, including the union of several of them.
        final String versionField = PseudoField.ROW_VERSION.getFieldName();
        assertThat(metaData.getPlannerTypeForRecordType(metaData.getRecordType(OUTER)).getFieldNameFieldMap())
                .containsKey(versionField);
        assertThat(metaData.getPlannerTypeForRecordTypes(
                        List.of(metaData.getRecordType(OUTER), metaData.getRecordType(OTHER))).getFieldNameFieldMap())
                .containsKey(versionField);
    }

    @Test
    void getPlannerTypeForRecordTypesUnionsTheirFields() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> { });
        final Type.Record unionType = metaData.getPlannerTypeForRecordTypes(
                List.of(metaData.getRecordType(OUTER), metaData.getRecordType(OTHER)));

        // `rec_id` and `other_id` are shared, so the union has them once; the remaining fields come from one type each.
        assertThat(fieldNames(unionType)).containsExactly("rec_id", "other_id", "map", "other_value");

        // A single type is described exactly as the type-taking overload would describe it on its own.
        assertThat(metaData.getPlannerTypeForRecordTypes(List.of(metaData.getRecordType(OUTER))))
                .isEqualTo(metaData.getPlannerTypeForRecordType(metaData.getRecordType(OUTER)));
    }

    @Nonnull
    private static RecordMetaData metaData(@Nonnull final Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static List<String> fieldNames(@Nonnull final Type.Record type) {
        return type.getFields().stream().map(Type.Record.Field::getFieldName).collect(Collectors.toList());
    }
}

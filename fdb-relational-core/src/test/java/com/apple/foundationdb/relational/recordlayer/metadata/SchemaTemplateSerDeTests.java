/*
 * SchemaTemplateSerDeTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;

import com.google.protobuf.DescriptorProtos;
import com.ibm.icu.impl.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

public class SchemaTemplateSerDeTests {

    private static RecordLayerSchemaTemplate getTestRecordLayerSchemaTemplate(@Nonnull Map<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>> template) {
        final var builder = RecordLayerSchemaTemplate.newBuilder().setName("TestSchemaTemplate");
        for (var entry : template.entrySet()) {
            final var tableBuilder = RecordLayerTable.newBuilder()
                    .setName(entry.getKey())
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setName(entry.getKey() + "_C")
                            .setDataType(DataType.Primitives.STRING.type())
                            .build());
            for (var generation : entry.getValue()) {
                tableBuilder.addGeneration(generation.first, generation.second);
            }
            builder.addTable(tableBuilder.build());
        }
        return builder.build();
    }

    @Test
    public void testGoodSchemaTemplate() {
        var testcase = new HashMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase.put("T1", List.of());
        testcase.put("T2", List.of());

        var template = getTestRecordLayerSchemaTemplate(testcase);
        var recordMetadataProto = template.toRecordMetadata().toProto();

        final var maybeUnionDesc = recordMetadataProto.getRecords().getMessageTypeList().stream()
                .filter(m -> "RecordTypeUnion".equals(m.getName()))
                .findFirst();
        Assertions.assertTrue(maybeUnionDesc.isPresent());
        final var unionDesc = maybeUnionDesc.get();

        // Check if all tables are part of union descriptor.
        final var expectedTableNameSet = Set.of("T1", "T2");
        Assertions.assertTrue(unionDesc.getFieldList().stream().allMatch(e -> expectedTableNameSet.contains(e.getTypeName())));

        // Check if the number of fields in union descriptor are equal to the tables in the template.
        final var expectedNumUnionFields = testcase.values().size();
        Assertions.assertEquals(expectedNumUnionFields, unionDesc.getFieldList().size());

        // Check if field numbers are assigned sequentially from [1, n]
        final var actualFieldNumbers = new HashSet<>();
        unionDesc.getFieldList().forEach(e -> actualFieldNumbers.add(e.getNumber()));
        for (var fieldNumber = 1; fieldNumber <= expectedNumUnionFields; fieldNumber++) {
            Assertions.assertTrue(actualFieldNumbers.contains(fieldNumber));
        }
    }

    @Test
    public void testGoodSchemaTemplateWithGenerations() {
        final var fieldOptions1 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(true).build();
        final var fieldOptions2 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(false).build();
        var testcase = new HashMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase.put("T1", List.of(Pair.of(1, fieldOptions1), Pair.of(2, fieldOptions2)));
        testcase.put("T2", List.of(Pair.of(3, fieldOptions2), Pair.of(4, fieldOptions1)));

        var template = getTestRecordLayerSchemaTemplate(testcase);
        var recordMetadataProto = template.toRecordMetadata().toProto();

        final var maybeUnionDesc = recordMetadataProto.getRecords().getMessageTypeList().stream()
                .filter(m -> "RecordTypeUnion".equals(m.getName()))
                .findFirst();
        Assertions.assertTrue(maybeUnionDesc.isPresent());
        final var unionDesc = maybeUnionDesc.get();

        // Check if the number of fields in union descriptor are equal to total number of generations across all tables.
        final var expectedUnionFields = testcase.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(expectedUnionFields, unionDesc.getFieldList().size());

        // Check if all generations are present in union descriptor
        for (var unionField : unionDesc.getFieldList()) {
            final var typeName = unionField.getTypeName();
            Assertions.assertTrue(testcase.containsKey(typeName));
            final var expectedGenerations = testcase.get(typeName);
            Assertions.assertTrue(expectedGenerations.contains(Pair.of(unionField.getNumber(), unionField.getOptions())));
        }
    }

    public static Stream<Arguments> badSchemaTemplateGenerationsTestcaseProvider() {
        final var fieldOptions1 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(true).build();
        final var fieldOptions2 = DescriptorProtos.FieldOptions.newBuilder().setDeprecated(false).build();

        // SchemaTemplate with field number 0
        var testcase1 = new TreeMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase1.put("T1", List.of(Pair.of(0, fieldOptions1), Pair.of(2, fieldOptions2)));
        // SchemaTemplate with duplicated field number
        var testcase2 = new TreeMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase2.put("T1", List.of(Pair.of(1, fieldOptions1), Pair.of(1, fieldOptions2)));
        // SchemaTemplate with duplicated fieldOptions
        var testcase3 = new TreeMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase3.put("T1", List.of(Pair.of(1, fieldOptions2), Pair.of(2, fieldOptions2)));
        // SchemaTemplate with duplicated field numbers across tables
        var testcase4 = new TreeMap<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>>();
        testcase4.put("T1", List.of(Pair.of(1, fieldOptions2), Pair.of(2, fieldOptions1)));
        testcase4.put("T2", List.of(Pair.of(2, fieldOptions2), Pair.of(3, fieldOptions1)));

        return Stream.of(
                Arguments.of(testcase1, UncheckedRelationalException.class, "Field numbers must be positive integers"),
                Arguments.of(testcase2, UncheckedRelationalException.class, "Duplicate field number 1 for generation of Table T1"),
                Arguments.of(testcase3, UncheckedRelationalException.class, "Duplicated options for different generations of Table T1"),
                Arguments.of(testcase4, UncheckedRelationalException.class, "Field number 2 has already been used")
        );
    }

    @ParameterizedTest
    @MethodSource("badSchemaTemplateGenerationsTestcaseProvider")
    public void testBadSchemaTemplateGenerations(Map<String, List<Pair<Integer, DescriptorProtos.FieldOptions>>> testcase,
                                                           Class<? extends Exception> exceptionClass, String message) {
        final var thrown = Assertions.assertThrows(exceptionClass, () -> {
            final var schemaTemplate = getTestRecordLayerSchemaTemplate(testcase);
            schemaTemplate.toRecordMetadata();
        });
        Assertions.assertTrue(thrown.getMessage().contains(message));
    }
}

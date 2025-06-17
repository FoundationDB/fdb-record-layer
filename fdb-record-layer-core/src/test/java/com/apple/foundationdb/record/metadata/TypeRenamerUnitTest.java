/*
 * TypeRenamerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TypeRenamerUnitTest {

    public static RecordMetaDataProto.MetaData.Builder loadMetaData(@Nonnull String name) throws IOException {
        try (@Nullable InputStream input = TypeRenamerUnitTest.class.getResourceAsStream("/" + name);
                InputStreamReader reader = new InputStreamReader(Objects.requireNonNull(input,
                        () -> "No resource: " + name))) {
            RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(reader, builder);
            return builder;
        }
    }

    @ParameterizedTest
    // Note: Explicitly having the .json here so that you can Cmd+click in the IDE to jump to the file
    @ValueSource(strings = {"OneBoringType.json",
            "TwoBoringTypes.json",
            "DuplicateUnionFields.json",
            "OneTypeWithIndexes.json",
            "MultiTypeIndex.json",
            "UniversalIndex.json",
            "UnnestedExternalType.json",
            "UnnestedInternal.json",
            "UnnestedRenamed.json",
            "Joined.json"
    })
    void simplePrefix(String name) throws IOException {
        runRename(name);
    }

    @Test
    void nestedAndRecordType() throws IOException {
        final RecordMetaData renamed = runRename("NestedAndRecordType.json");
        final Descriptors.FieldDescriptor t1Field = renamed.getRecordType("__x_T2").getDescriptor().getFields()
                .stream().filter(field -> field.getName().equals("T1"))
                .findFirst().orElseThrow();
        // if this assertion fails, it does not have a good toString, but you can add `.toProto()` to both for a better
        // toString
        assertEquals(renamed.getRecordsDescriptor().getMessageTypes().stream()
                        .filter(type -> type.getName().equals("__x_T1"))
                .findFirst().orElseThrow(),
                t1Field.getMessageType());
    }

    @Test
    void nestedMessage() throws IOException {
        final RecordMetaData renamed = runRename("NestedMessage.json");
        final Descriptors.FieldDescriptor t1Field = renamed.getRecordType("__x_T2").getDescriptor().getFields()
                .stream().filter(field -> field.getName().equals("T1"))
                .findFirst().orElseThrow();
        // if this assertion fails, it does not have a good toString, but you can add `.toProto()` to both for a better
        // toString
        assertEquals(renamed.getRecordsDescriptor().getMessageTypes().stream()
                        .filter(type -> type.getName().equals("T1"))
                        .findFirst().orElseThrow(),
                t1Field.getMessageType());
    }

    @Nonnull
    private RecordMetaData runRename(final String name) throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = loadMetaData(name);
        final RecordMetaDataProto.MetaData originalProto = builder.build();
        final RecordMetaData originalMetaData = RecordMetaData.build(originalProto);
        final Function<String, String> renamer = oldName -> "__x_" + oldName;
        final Function<String, String> undoRename = newName -> {
            assertEquals("__x_", newName.substring(0, 4));
            return newName.substring(4);
        };
        new TypeRenamer(renamer)
                .modify(builder, RecordMetaDataBuilder.getDependencies(originalProto, Map.of()));

        final RecordMetaData renamed = RecordMetaData.build(builder.build());
        final Set<String> expectedNewNames = originalMetaData.getRecordTypes().keySet()
                .stream().map(renamer)
                .collect(Collectors.toSet());
        assertEquals(expectedNewNames, renamed.getRecordTypes().keySet());
        assertEquals(expectedNewNames,
                renamed.getRecordTypes().values().stream().map(RecordType::getName)
                        .collect(Collectors.toSet()));
        for (final RecordType type : renamed.getRecordTypes().values()) {
            assertEquals(type.getAllIndexes(),
                    originalMetaData.getRecordType(undoRename.apply(type.getName()))
                            .getAllIndexes());
        }
        assertEquals(originalMetaData.getUniversalIndexes(), renamed.getUniversalIndexes());
        return renamed;
    }

}

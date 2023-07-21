/*
 * CommandUtil.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;

import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Util class for yaml-tests commands.
 */
public class CommandUtil {
    /**
     * Create a SchemaTemplate object from .proto
     *
     * @param loadCommandString input format is: "load: schema template ${SCHEMA_TEMPLATE_NAME} from ${PROTO_CLASS_NAME}"
     * @return a SchemaTemplate object
     */
    public static SchemaTemplate fromProto(String loadCommandString) {
        RecordMetaData metaData;
        Pair<String, String> templateNameAndSourceName = parseLoadTemplateString(loadCommandString);
        if (templateNameAndSourceName.getRight().endsWith(".json")) {
            metaData = loadRecordMetaDataFromJson(templateNameAndSourceName.getRight());
        } else {
            try {
                Class<?> act = Class.forName(templateNameAndSourceName.getRight());
                Method method = act.getMethod("getDescriptor");
                Descriptors.FileDescriptor o = (Descriptors.FileDescriptor) method.invoke(null);
                metaData = RecordMetaData.build(o);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                    ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return RecordLayerSchemaTemplate.fromRecordMetadata(metaData, templateNameAndSourceName.getLeft(), 1);
    }

    public static SchemaInstanceOuterClass.SchemaInstance fromJson(String loadCommandString) {
        SchemaInstanceOuterClass.SchemaInstance.Builder builder = SchemaInstanceOuterClass.SchemaInstance.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(loadCommandString, builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return builder.build();
    }

    public static Map<String, IndexState> fromIndexStateProto(Map<String, SchemaInstanceOuterClass.IndexState> indexStateProtoMap) {
        Map<String, IndexState> result = new HashMap<>();
        for (Map.Entry<String, SchemaInstanceOuterClass.IndexState> k : indexStateProtoMap.entrySet()) {
            result.put(k.getKey(), IndexState.fromCode((long) k.getValue().getNumber()));
        }
        return result;
    }

    private static RecordMetaData loadRecordMetaDataFromJson(String jsonFileName) {
        RecordMetaDataProto.MetaData.Builder builder = RecordMetaDataProto.MetaData.newBuilder();
        try {
            String json = Files.readString(Paths.get(jsonFileName), StandardCharsets.UTF_8);
            JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return RecordMetaData.build(builder.build());
    }

    private static Pair<String, String> parseLoadTemplateString(String loadCommandString) {
        StringTokenizer lcsTokenizer = new StringTokenizer(loadCommandString, " ");
        if (lcsTokenizer.countTokens() != 3) {
            Assertions.fail("Expecting load command consisting of 3 tokens");
        }
        String first = lcsTokenizer.nextToken();
        if (!"from".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load command looking like X from Y");
        }
        String second = lcsTokenizer.nextToken();
        return new ImmutablePair<>(first, second);
    }

    /**
     * Utility class that encapsulates <a href="https://en.wikipedia.org/wiki/ANSI_escape_code">ANSI escape sequences</a> for colors.
     */
    public enum Color {
        RESET("\u001B[0m"),
        BLACK("\u001B[30m"),
        RED("\u001B[31m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        BLUE("\u001B[34m"),
        PURPLE("\u001B[35m"),
        CYAN("\u001B[36m"),
        WHITE("\u001B[37m");

        @Nonnull
        private final String ansi;

        Color(@Nonnull final String ansi) {
            this.ansi = ansi;
        }

        @Override
        public String toString() {
            return ansi;
        }
    }
}

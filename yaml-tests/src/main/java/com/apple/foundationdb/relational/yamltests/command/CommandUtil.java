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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Util class for yaml-tests commands.
 */
public class CommandUtil {
    /**
     * Create a SchemaTemplate object from {@code .proto} file.
     *
     * @param loadCommandString input format is: "load: schema template ${SCHEMA_TEMPLATE_NAME} from ${PROTO_CLASS_NAME}"
     * @return a SchemaTemplate object
     */
    public static SchemaTemplate fromProto(String loadCommandString) {
        RecordMetaData metaData;
        Pair<String, String> templateTokens = parseLoadTemplateCommand(loadCommandString);
        if (templateTokens.getRight().endsWith(".json")) {
            metaData = loadRecordMetaDataFromJson(templateTokens.getRight());
        } else {
            try {
                Class<?> act = Class.forName(templateTokens.getRight());
                Method method = act.getMethod("getDescriptor");
                Descriptors.FileDescriptor o = (Descriptors.FileDescriptor) method.invoke(null);
                metaData = RecordMetaData.build(o);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                    ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return RecordLayerSchemaTemplate.fromRecordMetadata(metaData, templateTokens.getLeft(), 1);
    }

    private static Pair<String, String> parseLoadTemplateCommand(String loadCommandString) {
        StringTokenizer lcsTokenizer = new StringTokenizer(loadCommandString, " ");
        String templateName = lcsTokenizer.nextToken();
        if (!"from".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load command looking like X from Y");
        }
        String jsonFileName = lcsTokenizer.nextToken();
        return Pair.of(templateName, jsonFileName);
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
        List<String> deps = new ArrayList<>();
        try {
            String jsonStr = Files.readString(Paths.get(jsonFileName), StandardCharsets.UTF_8);
            JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, builder);
            JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
            JsonArray dependencyArray = obj.getAsJsonObject("records").getAsJsonArray("dependency");
            for (JsonElement element : dependencyArray) {
                String curDep = element.getAsString();
                if (!"record_metadata.proto".equals(curDep) && !"record_metadata_options.proto".equals(curDep)) {
                    deps.add(curDep);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();
        for (String dep: deps) {
            try {
                String fullClassName = getFullClassName(dep);
                Class<?> act = Class.forName(fullClassName);
                Method method = act.getMethod("getDescriptor");
                fileDescriptors.add((Descriptors.FileDescriptor) method.invoke(null));
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                     IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        return RecordMetaData.newBuilder()
                .addDependencies(fileDescriptors.toArray(new Descriptors.FileDescriptor[0]))
                .setRecords(builder.build())
                .getRecordMetaData();
    }

    private static String getFullClassName(String protoFileName) throws IOException {
        String fullProtoFileName = "src/test/proto/" + protoFileName;
        final Path path = Paths.get(fullProtoFileName);
        String content = Files.readString(path);

        // Match package
        Pattern packagePattern = Pattern.compile("package\\s+([\\w\\.]+);");
        Matcher packageMatcher = packagePattern.matcher(content);

        String packageName = null;
        if (packageMatcher.find()) {
            packageName = packageMatcher.group(1);
        }

        if (packageName == null) {
            throw new RuntimeException("Couldn't find package name in proto file " + fullProtoFileName);
        }

        // Match java_outer_classname
        Pattern outerClassPattern = Pattern.compile("option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\";");
        Matcher outerClassMatcher = outerClassPattern.matcher(content);

        String outerClassName = null;
        if (outerClassMatcher.find()) {
            outerClassName = outerClassMatcher.group(1);
        }

        // Optional fallback: default outer class if not explicitly defined
        if (outerClassName == null) {
            String fileName = path.getFileName().toString();
            outerClassName = fileName.replace(".proto", "").replaceAll("[^A-Za-z0-9]", "");
        }
        return packageName + "." + outerClassName;
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

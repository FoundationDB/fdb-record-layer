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
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        Set<String> neededDependencies = new LinkedHashSet<>();
        Set<String> includedDependencies = new HashSet<>();

        // These dependencies are automatically added, so we can treat them like they are bundled with the file dependencies
        includedDependencies.add("record_metadata.proto");
        includedDependencies.add("record_metadata_options.proto");
        includedDependencies.add("tuple_fields.proto");

        try {
            String jsonStr = Files.readString(Paths.get(jsonFileName), StandardCharsets.UTF_8);

            // Load the definition into the meta-data proto
            JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, builder);

            // Find the list of dependencies of the top-level file
            JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
            JsonArray dependencyArray = obj.getAsJsonObject("records").getAsJsonArray("dependency");
            for (JsonElement element : dependencyArray) {
                String curDep = element.getAsString();
                neededDependencies.add(curDep);
            }

            // Some dependencies may be included in the JSON descriptor itself and do not need to be
            // provided from the environment
            JsonArray includedDependencyDefinitions = obj.getAsJsonArray("dependencies");
            if (includedDependencyDefinitions != null) {
                for (JsonElement element : includedDependencyDefinitions) {
                    JsonObject definition = element.getAsJsonObject();
                    includedDependencies.add(definition.get("name").getAsString());
                    if (definition.has("dependency")) {
                        definition.getAsJsonArray("dependency")
                                .forEach(dep -> neededDependencies.add(dep.getAsString()));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();
        for (String dep: neededDependencies) {
            if (includedDependencies.contains(dep)) {
                continue;
            }
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

    private static String getFullClassName(String protoFileName) throws IOException {
        String fullProtoFileName = "src/test/proto/" + protoFileName;
        final Path path = Paths.get(fullProtoFileName);
        String content = Files.readString(path);

        // Match 'package my.package.name;'
        Pattern packagePattern = Pattern.compile("package\\s+([\\w\\.]+);");
        Matcher packageMatcher = packagePattern.matcher(content);

        String protoPackage;
        if (packageMatcher.find()) {
            protoPackage = packageMatcher.group(1);
        } else {
            throw new IllegalArgumentException("unable to find package name in proto file:" + fullProtoFileName);
        }

        // Match 'option java_package = "com.example";'
        Pattern javaPackagePattern = Pattern.compile("option\\s+java_package\\s*=\\s*\"([^\"]+)\";");
        Matcher javaPackageMatcher = javaPackagePattern.matcher(content);

        String javaPackage = null;
        if (javaPackageMatcher.find()) {
            javaPackage = javaPackageMatcher.group(1);
        }

        // Final package name: java_package if exists, otherwise proto package
        String effectivePackage = javaPackage != null ? javaPackage : protoPackage;

        // Match java_outer_classname
        Pattern outerClassPattern = Pattern.compile("option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\";");
        Matcher outerClassMatcher = outerClassPattern.matcher(content);

        String outerClassName;
        if (outerClassMatcher.find()) {
            outerClassName = outerClassMatcher.group(1);
        } else {
            // fallback: default outer class if not explicitly defined
            outerClassName = protoFileName.replace(".proto", "").replaceAll("[^A-Za-z0-9]", "");
        }
        return effectivePackage + "." + outerClassName;
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

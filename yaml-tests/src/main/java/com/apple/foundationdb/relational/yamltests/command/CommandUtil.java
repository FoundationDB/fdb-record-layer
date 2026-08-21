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
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.yamltests.generated.schemainstance.SchemaInstanceOuterClass;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

        final JsonObject obj;
        try {
            String jsonStr = Files.readString(Paths.get(jsonFileName), StandardCharsets.UTF_8);

            // Load the definition into the meta-data proto
            JsonFormat.parser().ignoringUnknownFields().merge(jsonStr, builder);

            // Find the list of dependencies of the top-level file
            obj = JsonParser.parseString(jsonStr).getAsJsonObject();
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
        List<Class<?>> dependencyClasses = new ArrayList<>();
        for (String dep: neededDependencies) {
            if (includedDependencies.contains(dep)) {
                continue;
            }
            try {
                String fullClassName = getFullClassName(dep);
                Class<?> act = Class.forName(fullClassName);
                Method method = act.getMethod("getDescriptor");
                fileDescriptors.add((Descriptors.FileDescriptor) method.invoke(null));
                dependencyClasses.add(act);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                     IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        mergeExtensions(builder, obj, extensionRegistry(dependencyClasses));

        return RecordMetaData.newBuilder()
                .addDependencies(fileDescriptors.toArray(new Descriptors.FileDescriptor[0]))
                .setRecords(builder.build())
                .getRecordMetaData();
    }

    /**
     * Collect the extensions declared by the meta-data protos themselves and by every resolved dependency, so that the
     * extensions the JSON carries can be looked up by their full name, see
     * {@link #mergeExtensions(Message.Builder, JsonObject, ExtensionRegistry)}.
     *
     * @param dependencyClasses the generated outer classes of the resolved dependencies
     * @return a registry holding all extensions those classes declare
     */
    @Nonnull
    private static ExtensionRegistry extensionRegistry(@Nonnull List<Class<?>> dependencyClasses) {
        final ExtensionRegistry registry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(registry);
        for (Class<?> dependencyClass : dependencyClasses) {
            for (Method method : dependencyClass.getMethods()) {
                // a generated class without any extension to register does not have that method at all
                if (!"registerAllExtensions".equals(method.getName())
                        || !Arrays.equals(method.getParameterTypes(), new Class<?>[] {ExtensionRegistry.class})) {
                    continue;
                }
                try {
                    method.invoke(null, registry);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return registry;
    }

    /**
     * Re-attach the proto2 extensions that {@link JsonFormat} discarded while parsing the meta-data.
     * <p>
     * {@code JsonFormat} implements the proto3 JSON mapping, which has no notion of extensions: they are treated as if
     * they did not exist, both when the meta-data is written and when it is read back. Extensions do, however, carry
     * information the meta-data cannot do without. A vector field, for instance, is a {@code bytes} field whose
     * dimensions and precision live in an extension of {@code google.protobuf.FieldOptions}, so dropping the extension
     * silently turns the field into a plain {@code bytes} field:
     * <pre>{@code
     * "embedding": { "type": "TYPE_BYTES",
     *                "options": { "com.apple.foundationdb.record.field": {
     *                             "vectorOptions": { "precision": 64, "dimensions": 512 }}}}
     * }</pre>
     * The value of an extension is itself an ordinary message, though, so this walks the JSON alongside the builder it
     * was parsed into, and for every key naming a known extension of the message at hand, parses that value and sets it
     * on the builder.
     * </p>
     *
     * @param builder the builder the {@code json} object was parsed into
     * @param json the JSON object the builder was parsed from
     * @param registry the extensions to look the keys of {@code json} up in
     */
    private static void mergeExtensions(@Nonnull Message.Builder builder,
                                        @Nonnull JsonObject json,
                                        @Nonnull ExtensionRegistry registry) {
        final Descriptors.Descriptor descriptor = builder.getDescriptorForType();
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            final ExtensionRegistry.ExtensionInfo extension = registry.findImmutableExtensionByName(entry.getKey());
            if (extension != null && descriptor.equals(extension.descriptor.getContainingType())) {
                setExtension(builder, extension.descriptor, extension.defaultInstance, entry.getValue());
                continue;
            }
            // not an extension of this message, but it may well be a message holding extensions further down
            final Descriptors.FieldDescriptor field = findField(descriptor, entry.getKey());
            if (field == null || field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.isRepeated()) {
                final JsonArray elements = entry.getValue().getAsJsonArray();
                // the builder was parsed from this very array, so the two are in the same order
                for (int i = 0; i < Math.min(elements.size(), builder.getRepeatedFieldCount(field)); i++) {
                    mergeExtensions(builder.getRepeatedFieldBuilder(field, i), elements.get(i).getAsJsonObject(),
                            registry);
                }
            } else {
                mergeExtensions(builder.getFieldBuilder(field), entry.getValue().getAsJsonObject(), registry);
            }
        }
    }

    /**
     * Parse the JSON representation of an extension value and set it on the given builder.
     *
     * @param builder the builder to set the extension on
     * @param extension the field descriptor of the extension
     * @param defaultInstance the default instance of the extension value, {@code null} unless it is a message
     * @param value the JSON representation of the extension value
     */
    private static void setExtension(@Nonnull Message.Builder builder,
                                     @Nonnull Descriptors.FieldDescriptor extension,
                                     @Nullable Message defaultInstance,
                                     @Nonnull JsonElement value) {
        if (extension.isRepeated()) {
            for (JsonElement element : value.getAsJsonArray()) {
                builder.addRepeatedField(extension, extensionValue(extension, defaultInstance, element));
            }
        } else {
            builder.setField(extension, extensionValue(extension, defaultInstance, value));
        }
    }

    @Nonnull
    private static Object extensionValue(@Nonnull Descriptors.FieldDescriptor extension,
                                         @Nullable Message defaultInstance,
                                         @Nonnull JsonElement value) {
        switch (extension.getJavaType()) {
            case MESSAGE:
                // the fields of the extension value are regular fields, so JsonFormat handles them just fine
                final Message.Builder valueBuilder = Objects.requireNonNull(defaultInstance).newBuilderForType();
                try {
                    JsonFormat.parser().ignoringUnknownFields().merge(value.toString(), valueBuilder);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("unable to parse value of extension " + extension.getFullName(), e);
                }
                return valueBuilder.build();
            case BOOLEAN:
                return value.getAsBoolean();
            case INT:
                return value.getAsInt();
            case LONG:
                return value.getAsLong();
            case FLOAT:
                return value.getAsFloat();
            case DOUBLE:
                return value.getAsDouble();
            case STRING:
                return value.getAsString();
            case BYTE_STRING:
                return ByteString.copyFrom(Base64.getDecoder().decode(value.getAsString()));
            case ENUM:
                final Descriptors.EnumValueDescriptor enumValue =
                        extension.getEnumType().findValueByName(value.getAsString());
                return Objects.requireNonNull(enumValue, () -> "unknown value of enum extension "
                        + extension.getFullName() + ": " + value);
            default:
                throw new RuntimeException("unsupported type of extension " + extension.getFullName());
        }
    }

    /**
     * Find a field by the name the proto3 JSON mapping allows for it, which is either the name it is declared with or
     * its lower camel case form.
     *
     * @param descriptor the message to look the field up in
     * @param name the name of the field as it appears in the JSON
     * @return the field, or {@code null} if the message has no such field
     */
    @Nullable
    private static Descriptors.FieldDescriptor findField(@Nonnull Descriptors.Descriptor descriptor,
                                                        @Nonnull String name) {
        final Descriptors.FieldDescriptor byName = descriptor.findFieldByName(name);
        if (byName != null) {
            return byName;
        }
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            if (field.getJsonName().equals(name)) {
                return field;
            }
        }
        return null;
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
        return Pair.of(first, second);
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

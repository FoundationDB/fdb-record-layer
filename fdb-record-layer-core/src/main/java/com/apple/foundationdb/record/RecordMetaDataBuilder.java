/*
 * RecordMetaDataBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistryImpl;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

/**
 * A builder for {@link RecordMetaData}.
 *
 * Meta-data can be built in two ways.
 *
 * <p>
 * <b>From compiled .proto</b><br>
 * Simple single field indexes and single field primary keys can be specified in the Protobuf source file using option extensions.
 * Additional indexes or more complicated primary keys need to be specified with code using this builder.
 * The {@link #setRecords(Descriptors.FileDescriptor, boolean)} method loads the meta-data information from the Protobuf source file. Indexes and other
 * properties such as version are not accessible before calling {@code setRecords}.
 * </p>
 *
 * <p>
 * <b>From a {@link RecordMetaDataProto.MetaData} Protobuf message</b><br>
 * The Protobuf form can store the complete meta-data. The {@link #setRecords(RecordMetaDataProto.MetaData, boolean)} method loads
 * the Protobuf message. Similar to loading the meta-data directly from a Protobuf file descriptor, indexes and other
 * properties are not accessible before calling {@code setRecords}.
 * The Protobuf message may contain all of the dependencies required for resolving the record types and indexes. If some
 * of the dependencies are missing (e.g., when a list of excluded dependencies is passed to {@link RecordMetaData#toProto}), before calling
 * {@code setRecords}, callers must first add the missing dependencies using
 * {@link #addDependency(Descriptors.FileDescriptor)} or {@link #addDependencies(Descriptors.FileDescriptor[])}.
 * The {@code addDependency} or {@code addDependencies}
 * methods can also be used to override the embedded dependencies.
 * @see RecordMetaData#toProto
 * </p>
 *
 */
@API(API.Status.MAINTAINED)
public class RecordMetaDataBuilder implements RecordMetaDataProvider {

    private static final Logger logger = LoggerFactory.getLogger(RecordMetaDataBuilder.class);
    private static final Descriptors.FileDescriptor[] emptyDependencyList = new Descriptors.FileDescriptor[0];

    @Nullable
    private Descriptors.FileDescriptor recordsDescriptor;
    @Nullable
    private Descriptors.Descriptor unionDescriptor;
    @Nonnull
    private final Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> unionFields;
    @Nonnull
    private final Map<String, RecordTypeBuilder> recordTypes;
    @Nonnull
    private final Map<String, Index> indexes;
    @Nonnull
    private final Map<String, Index> universalIndexes;
    private boolean splitLongRecords;
    private boolean storeRecordVersions;
    private int version;
    @Nonnull
    private final List<FormerIndex> formerIndexes;
    @Nonnull
    private IndexMaintainerRegistry indexMaintainerRegistry;
    @Nullable
    private KeyExpression recordCountKey;
    @Nullable
    private RecordMetaData recordMetaData;
    @Nonnull
    private final Map<String, Descriptors.FileDescriptor> explicitDependencies;

    /**
     * Creates a blank builder.
     */
    RecordMetaDataBuilder() {
        recordTypes = new HashMap<>();
        indexes = new HashMap<>();
        universalIndexes = new HashMap<>();
        formerIndexes = new ArrayList<>();
        unionFields = new HashMap<>();
        explicitDependencies = new TreeMap<>();
        indexMaintainerRegistry = IndexMaintainerRegistryImpl.instance();
    }

    /**
     * Creates a new builder from the provided record types protobuf.
     * @param fileDescriptor a file descriptor containing all the record types in the metadata
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        this(fileDescriptor, true);
    }

    /**
     * Creates a new builder from the provided record types protobuf.
     * @param fileDescriptor a file descriptor containing all the record types in the metadata
     * @param processExtensionOptions whether to add primary keys and indexes based on extensions in the protobuf
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                 boolean processExtensionOptions) {
        this();
        loadFromFileDescriptor(fileDescriptor, processExtensionOptions);
    }

    /**
     * Creates a new builder from the provided meta-data protobuf.
     *
     * This constructor assumes that {@code metaDataProto} is not the result of {@link RecordMetaData#toProto} and will not already
     * include all the indexes defined by any original extension options, so that they still need to be processed.
     * If {@code metaDataProto} is the result of {@code toProto} and indexes also appear in extension options, a duplicate index
     * error will result. In that case, {@link #RecordMetaDataBuilder(RecordMetaDataProto.MetaData, boolean)} will be needed instead.
     *
     * @param metaDataProto the protobuf form of the meta-data
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        this(metaDataProto, true);
    }

    /**
     * Creates a new builder from the provided meta-data protobuf.
     *
     * If {@code metaDataProto} is the result of {@link RecordMetaData#toProto}, it will already
     * include all the indexes defined by any original extension options, so {@code processExtensionOptions}
     * should be {@code false}.
     *
     * @param metaDataProto the protobuf form of the meta-data
     * @param processExtensionOptions whether to add primary keys and indexes based on extensions in the protobuf
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                                 boolean processExtensionOptions) {
        this(metaDataProto, new Descriptors.FileDescriptor[] { RecordMetaDataOptionsProto.getDescriptor() }, processExtensionOptions);
    }

    /**
     * Creates a new builder from the provided meta-data protobuf.
     *
     * This constructor assumes that {@code metaDataProto} is not the result of {@link RecordMetaData#toProto} and will not already
     * include all the indexes defined by any original extension options, so that they still need to be processed.
     * If {@code metaDataProto} is the result of {@code toProto} and indexes also appear in extension options, a duplicate index
     * error will result. In that case, {@link #RecordMetaDataBuilder(RecordMetaDataProto.MetaData, Descriptors.FileDescriptor[], boolean)} will be needed instead.
     *
     * @param metaDataProto the protobuf form of the meta-data
     * @param dependencies other files imported by the record types protobuf
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                                 @Nonnull Descriptors.FileDescriptor[] dependencies) {
        this(metaDataProto, dependencies, true);
    }

    /**
     * Creates a new builder from the provided meta-data protobuf.
     *
     * If {@code metaDataProto} is the result of {@link RecordMetaData#toProto}, it will already
     * include all the indexes defined by any original extension options, so {@code processExtensionOptions}
     * should be {@code false}.
     *
     * @param metaDataProto the protobuf form of the meta-data
     * @param dependencies other files imported by the record types protobuf
     * @param processExtensionOptions whether to add primary keys and indexes based on extensions in the protobuf
     * @deprecated use {@link RecordMetaData#newBuilder()} instead
     */
    @Deprecated
    public RecordMetaDataBuilder(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                                 @Nonnull Descriptors.FileDescriptor[] dependencies,
                                 boolean processExtensionOptions) {
        this();
        loadFromProto(metaDataProto, dependencies, processExtensionOptions);
    }

    private void processSchemaOptions(boolean processExtensionOptions) {
        if (processExtensionOptions) {
            RecordMetaDataOptionsProto.SchemaOptions schemaOptions = recordsDescriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.schema);
            if (schemaOptions != null) {
                if (schemaOptions.hasSplitLongRecords()) {
                    splitLongRecords = schemaOptions.getSplitLongRecords();
                }
                if (schemaOptions.hasStoreRecordVersions()) {
                    storeRecordVersions = schemaOptions.getStoreRecordVersions();
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void loadProtoExceptRecords(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        for (RecordMetaDataProto.Index indexProto : metaDataProto.getIndexesList()) {
            List<RecordTypeBuilder> recordTypeBuilders = new ArrayList<>(indexProto.getRecordTypeCount());
            for (String recordTypeName : indexProto.getRecordTypeList()) {
                recordTypeBuilders.add(getRecordType(recordTypeName));
            }
            try {
                addMultiTypeIndex(recordTypeBuilders, new Index(indexProto));
            } catch (KeyExpression.DeserializationException e) {
                throw new MetaDataProtoDeserializationException(e);
            }
        }
        for (RecordMetaDataProto.RecordType typeProto : metaDataProto.getRecordTypesList()) {
            RecordTypeBuilder typeBuilder = getRecordType(typeProto.getName());
            if (typeProto.hasPrimaryKey()) {
                try {
                    typeBuilder.setPrimaryKey(KeyExpression.fromProto(typeProto.getPrimaryKey()));
                } catch (KeyExpression.DeserializationException e) {
                    throw new MetaDataProtoDeserializationException(e);
                }
            }
            if (typeProto.hasSinceVersion()) {
                typeBuilder.setSinceVersion(typeProto.getSinceVersion());
            }
            if (typeProto.hasExplicitKey()) {
                typeBuilder.setRecordTypeKey(LiteralKeyExpression.fromProtoValue(typeProto.getExplicitKey()));
            }
        }
        if (metaDataProto.hasSplitLongRecords()) {
            splitLongRecords = metaDataProto.getSplitLongRecords();
        }
        if (metaDataProto.hasStoreRecordVersions()) {
            storeRecordVersions = metaDataProto.getStoreRecordVersions();
        }
        for (RecordMetaDataProto.FormerIndex formerIndex : metaDataProto.getFormerIndexesList()) {
            formerIndexes.add(new FormerIndex(formerIndex));
        }
        if (metaDataProto.hasRecordCountKey()) {
            try {
                recordCountKey = KeyExpression.fromProto(metaDataProto.getRecordCountKey());
            } catch (KeyExpression.DeserializationException e) {
                throw new MetaDataProtoDeserializationException(e);
            }
        }
        if (metaDataProto.hasVersion()) {
            version = metaDataProto.getVersion();
        }
    }

    private void loadFromProto(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                               @Nonnull Descriptors.FileDescriptor[] dependencies,
                               boolean processExtensionOptions) {
        recordsDescriptor = buildFileDescriptor(metaDataProto.getRecords(), dependencies);
        validateRecords(recordsDescriptor);
        unionDescriptor = initRecordTypes(processExtensionOptions);
        loadProtoExceptRecords(metaDataProto);
        processSchemaOptions(processExtensionOptions);
    }

    private void loadFromFileDescriptor(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                        boolean processExtensionOptions) {
        recordsDescriptor = fileDescriptor;
        validateRecords(fileDescriptor);
        unionDescriptor = initRecordTypes(processExtensionOptions);
        processSchemaOptions(processExtensionOptions);
    }

    @Nonnull
    private Map<String, Descriptors.FileDescriptor> initGeneratedDependencies(@Nonnull Map<String, DescriptorProtos.FileDescriptorProto> protoDependencies) {
        Map<String, Descriptors.FileDescriptor> generatedDependencies = new TreeMap<>();
        if (!protoDependencies.containsKey(TupleFieldsProto.getDescriptor().getName())) {
            generatedDependencies.put(TupleFieldsProto.getDescriptor().getName(), TupleFieldsProto.getDescriptor());
        }
        if (!protoDependencies.containsKey(RecordMetaDataOptionsProto.getDescriptor().getName())) {
            generatedDependencies.put(RecordMetaDataOptionsProto.getDescriptor().getName(), RecordMetaDataOptionsProto.getDescriptor());
        }
        if (!protoDependencies.containsKey(RecordMetaDataProto.getDescriptor().getName())) {
            generatedDependencies.put(RecordMetaDataProto.getDescriptor().getName(), RecordMetaDataProto.getDescriptor());
        }
        return generatedDependencies;
    }

    /**
     * Deserializes the meta-data proto into the builder. The extension options are not processed.
     * @param metaDataProto the proto of the {@link RecordMetaData}
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder setRecords(@Nonnull RecordMetaDataProto.MetaData metaDataProto) {
        return setRecords(metaDataProto, false);
    }

    /**
     * Deserializes the meta-data proto into the builder.
     * @param metaDataProto the proto of the {@link RecordMetaData}
     * @param processExtensionOptions whether to add primary keys and indexes based on extensions in the protobuf
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder setRecords(@Nonnull RecordMetaDataProto.MetaData metaDataProto,
                                            boolean processExtensionOptions) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set.");
        }
        // Build the recordDescriptor by de-serializing the metaData proto
        Map<String, DescriptorProtos.FileDescriptorProto> protoDependencies = new TreeMap<>();
        for (DescriptorProtos.FileDescriptorProto dependency : metaDataProto.getDependenciesList()) {
            protoDependencies.put(dependency.getName(), dependency);
        }
        Map<String, Descriptors.FileDescriptor> generatedDependencies = initGeneratedDependencies(protoDependencies);
        Descriptors.FileDescriptor[] dependencies = getDependencies(metaDataProto.getRecords(), generatedDependencies, protoDependencies);
        loadFromProto(metaDataProto, dependencies, processExtensionOptions);
        return this;
    }

    /**
     * Adds the root file descriptor of the {@link RecordMetaData} and processes the extension options.
     * @param fileDescriptor the file descriptor of the record meta-data
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder setRecords(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        return setRecords(fileDescriptor, true);
    }

    /**
     * Adds the root file descriptor of the {@link RecordMetaData}.
     * @param fileDescriptor the file descriptor of the record meta-data
     * @param processExtensionOptions whether to add primary keys and indexes based on extensions in the protobuf
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder setRecords(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                            boolean processExtensionOptions) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set.");
        }
        loadFromFileDescriptor(fileDescriptor, processExtensionOptions);
        return this;
    }

    /**
     * Adds a dependency to the list of dependencies. It will be used for loading the {@link RecordMetaData} from a meta-data proto.
     * @param fileDescriptor the file descriptor of the dependency
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder addDependency(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set. Adding dependencies not allowed.");
        }
        explicitDependencies.put(fileDescriptor.getName(), fileDescriptor);
        return this;
    }

    /**
     * Adds dependencies to be used for loading the {@link RecordMetaData} from a meta-data proto.
     * @param fileDescriptors a list of dependencies
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder addDependencies(@Nonnull Descriptors.FileDescriptor[] fileDescriptors) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set. Adding dependencies not allowed.");
        }
        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptors) {
            explicitDependencies.put(fileDescriptor.getName(), fileDescriptor);
        }
        return this;
    }

    private Descriptors.FileDescriptor[] getDependencies(@Nonnull DescriptorProtos.FileDescriptorProto proto,
                                                         @Nonnull Map<String, Descriptors.FileDescriptor> generatedDependencies,
                                                         @Nonnull Map<String, DescriptorProtos.FileDescriptorProto> protoDependencies) {
        if (proto.getDependencyCount() == 0) {
            return emptyDependencyList;
        }

        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[proto.getDependencyCount()];
        for (int index = 0; index < proto.getDependencyCount(); index++) {
            String key = proto.getDependency(index);
            if (this.explicitDependencies.containsKey(key)) {
                // Provided by caller.
                dependencies[index] = this.explicitDependencies.get(key);
            } else if (generatedDependencies.containsKey(key)) {
                // Generated already.
                dependencies[index] = generatedDependencies.get(key);
            } else if (protoDependencies.containsKey(key)) {
                // Not seen before. Build it.
                DescriptorProtos.FileDescriptorProto dependency = protoDependencies.get(key);
                dependencies[index] = buildFileDescriptor(dependency, getDependencies(dependency, generatedDependencies, protoDependencies));
                generatedDependencies.put(key, dependencies[index]);
            } else {
                // Unknown dependency.
                throw new MetaDataException(String.format("Dependency %s not found", key));
            }
        }
        return dependencies;
    }

    private static void validateRecords(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        Queue<Descriptors.Descriptor> toValidate = new ArrayDeque<>(fileDescriptor.getMessageTypes());
        Set<Descriptors.Descriptor> seen = new HashSet<>();
        while (!toValidate.isEmpty()) {
            Descriptors.Descriptor descriptor = toValidate.remove();
            if (seen.add(descriptor)) {
                for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                    switch (field.getType()) {
                        case INT32:
                        case INT64:
                        case SFIXED32:
                        case SFIXED64:
                        case SINT32:
                        case SINT64:
                        case BOOL:
                        case STRING:
                        case BYTES:
                        case FLOAT:
                        case DOUBLE:
                        case ENUM:
                            // These types are allowed ; nothing to do.
                            break;
                        case MESSAGE:
                        case GROUP:
                            if (!seen.contains(field.getMessageType())) {
                                toValidate.add(field.getMessageType());
                            }
                            break;
                        case FIXED32:
                        case FIXED64:
                        case UINT32:
                        case UINT64:
                            throw new MetaDataException(
                                    "Field " + field.getName()
                                    + " in message " + descriptor.getFullName()
                                    + " has illegal unsigned type " + field.getType().name());
                        default:
                            throw new MetaDataException(
                                    "Field " + field.getName()
                                    + " in message " + descriptor.getFullName()
                                    + " has unknown type " + field.getType().name());
                    }
                }
            }
        }
    }

    private Descriptors.Descriptor initRecordTypes(boolean processExtensionOptions) {
        Descriptors.Descriptor union = null;
        for (Descriptors.Descriptor descriptor : recordsDescriptor.getMessageTypes()) {
            @Nullable Integer sinceVersion = null;
            @Nullable Object recordTypeKey = null;
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null) {
                switch (recordTypeOptions.getUsage()) {
                    case UNION:
                        if (union != null) {
                            throw new MetaDataException("Only one union descriptor is allowed");
                        }
                        union = descriptor;
                        continue;
                    case NESTED:
                        continue;
                    case RECORD:
                    default:
                        break;
                }
                if (processExtensionOptions && recordTypeOptions.hasSinceVersion()) {
                    sinceVersion = recordTypeOptions.getSinceVersion();
                }
                if (processExtensionOptions && recordTypeOptions.hasRecordTypeKey()) {
                    recordTypeKey = LiteralKeyExpression.fromProto(recordTypeOptions.getRecordTypeKey()).getValue();
                }
            }
            if ("RecordTypeUnion".equals(descriptor.getName())) {
                if (union != null) {
                    throw new MetaDataException("Only one union descriptor is allowed");
                }
                union = descriptor;
                continue;
            }

            RecordTypeBuilder recordType = new RecordTypeBuilder(descriptor);
            recordTypes.put(recordType.getName(), recordType);
            if (processExtensionOptions) {
                recordType.setSinceVersion(sinceVersion);
                recordType.setRecordTypeKey(recordTypeKey);
                protoFieldOptions(recordType, descriptor);
            }
        }
        if (union == null) {
            throw new MetaDataException("Union descriptor is required");
        }
        fillUnionFields(union, processExtensionOptions);
        return union;
    }

    private void fillUnionFields(Descriptors.Descriptor union, boolean processExtensionOptions) {
        for (Descriptors.FieldDescriptor unionField : union.getFields()) {
            if (unionField.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
                throw new MetaDataException("Union field " + unionField.getName() +
                                            " is not a message");
            }
            if (unionField.isRepeated()) {
                throw new MetaDataException("Union field " + unionField.getName() +
                                            " should not be repeated");
            }
            Descriptors.Descriptor descriptor = unionField.getMessageType();
            if (!unionFields.containsKey(descriptor)) {
                RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                        .getExtension(RecordMetaDataOptionsProto.record);
                if (recordTypeOptions != null &&
                        recordTypeOptions.getUsage() != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.RECORD) {
                    throw new MetaDataException("Union field " + unionField.getName() +
                                                " has type " + descriptor.getName() +
                                                " which is not a record");
                }

                if (descriptor.getFile() != recordsDescriptor) {
                    // An imported record type.
                    RecordTypeBuilder recordType = new RecordTypeBuilder(descriptor);
                    if (recordTypes.putIfAbsent(recordType.getName(), recordType) != null) {
                        throw new MetaDataException("There is already a record type named" + recordType.getName());
                    }
                    if (processExtensionOptions) {
                        protoFieldOptions(recordType, descriptor);
                    }
                }

                unionFields.put(descriptor, unionField);
            } else {
                // The preferred field is the last one, except if there is one whose name matches.
                unionFields.compute(descriptor, (d, f) -> f != null && f.getName().equals("_" + d.getName()) ? f : unionField);
            }
        }
    }

    private void protoFieldOptions(RecordTypeBuilder recordType, Descriptors.Descriptor descriptor) {
        // Add indexes from custom options.
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            RecordMetaDataOptionsProto.FieldOptions fieldOptions = fieldDescriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.field);
            if (fieldOptions != null) {
                protoFieldOptions(recordType, descriptor, fieldDescriptor, fieldOptions);
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void protoFieldOptions(RecordTypeBuilder recordType, Descriptors.Descriptor descriptor,
                                   Descriptors.FieldDescriptor fieldDescriptor, RecordMetaDataOptionsProto.FieldOptions fieldOptions) {
        if (fieldOptions.hasIndex() || fieldOptions.hasIndexed()) {
            String type;
            Map<String, String> options;
            if (fieldOptions.hasIndex()) {
                RecordMetaDataOptionsProto.FieldOptions.IndexOption indexOption = fieldOptions.getIndex();
                type = indexOption.getType();
                options = Index.buildOptions(indexOption.getOptionsList(), indexOption.getUnique());
            } else {
                type = Index.indexTypeToType(fieldOptions.getIndexed());
                options = Index.indexTypeToOptions(fieldOptions.getIndexed());
            }
            final FieldKeyExpression field = Key.Expressions.fromDescriptor(fieldDescriptor);
            final KeyExpression expr;
            if (type.equals(IndexTypes.RANK)) {
                expr = field.ungrouped();
            } else {
                expr = field;
            }
            final Index index = new Index(descriptor.getName() + "$" + fieldDescriptor.getName(),
                    expr, Index.EMPTY_VALUE, type, options);
            addIndex(recordType, index);
        } else if (fieldOptions.getPrimaryKey()) {
            if (recordType.getPrimaryKey() != null) {
                throw new MetaDataException("Only one primary key per record type is allowed have: " +
                                            recordType.getPrimaryKey() + "; adding on " + fieldDescriptor.getName());
            } else {
                if (fieldDescriptor.isRepeated()) {
                    // TODO maybe default to concatenate for this.
                    throw new MetaDataException("Primary key cannot be set on a repeated field");
                } else {
                    recordType.setPrimaryKey(Key.Expressions.fromDescriptor(fieldDescriptor));
                }
            }
        }
    }

    private static Descriptors.FileDescriptor buildFileDescriptor(@Nonnull DescriptorProtos.FileDescriptorProto fileDescriptorProto,
                                                                  @Nonnull Descriptors.FileDescriptor[] dependencies) {
        try {
            return Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new MetaDataException("Error converting from protobuf", ex);
        }
    }

    @Nonnull
    public Descriptors.Descriptor getUnionDescriptor() {
        return unionDescriptor;
    }

    @Nonnull
    public Descriptors.FieldDescriptor getUnionFieldForRecordType(@Nonnull RecordType recordType) {
        final Descriptors.FieldDescriptor unionField = unionFields.get(recordType.getDescriptor());
        if (unionField == null) {
            throw new MetaDataException("Record type " + recordType.getName() + " is not in the union");
        }
        return unionField;
    }

    @Nonnull
    public RecordTypeBuilder getRecordType(@Nonnull String name) {
        RecordTypeBuilder recordType = recordTypes.get(name);
        if (recordType == null) {
            throw new MetaDataException("Unknown record type " + name);
        }
        return recordType;
    }

    @Nonnull
    public Index getIndex(@Nonnull String indexName) {
        Index index = indexes.get(indexName);
        if (null == index) {
            throw new MetaDataException("Index " + indexName + " not defined");
        }
        return index;
    }

    // Common code shared by all the methods that add indexes. It runs some validation
    // and bumps the version if necessary.
    private void addIndexCommon(@Nonnull Index index) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("No records added yet");
        }
        if (indexes.containsKey(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " already defined");
        }
        if (index.getLastModifiedVersion() <= 0) {
            index.setLastModifiedVersion(++version);
        } else if (index.getLastModifiedVersion() > version) {
            version = index.getLastModifiedVersion();
        }
        if (index.getAddedVersion() <= 0) {
            index.setAddedVersion(index.getLastModifiedVersion());
        }
        indexes.put(index.getName(), index);
    }

    /**
     * Adds a new index. This index can either be a universal index or an index for
     * a single record type.
     * @param recordType if null this index will exist for all record types
     * @param index the index to be added
     */
    public void addIndex(@Nullable RecordTypeBuilder recordType, @Nonnull Index index) {
        addIndexCommon(index);
        if (recordType != null) {
            recordType.getIndexes().add(index);
        } else {
            universalIndexes.put(index.getName(), index);
        }
    }

    /**
     * Adds a new index.
     * @param recordType name of the record type
     * @param index the index to be added
     */
    public void addIndex(@Nonnull String recordType, @Nonnull Index index) {
        addIndex(getRecordType(recordType), index);
    }

    /**
     * Adds a new index.
     * @param recordType name of the record type
     * @param indexName the name of the new index
     * @param indexExpression the root expression of the new index
     */
    public void addIndex(@Nonnull String recordType, @Nonnull String indexName, @Nonnull KeyExpression indexExpression) {
        addIndex(recordType, new Index(indexName, indexExpression));
    }

    /**
     * Adds a new index.
     * @param recordType name of the record type
     * @param indexName the name of the new index
     * @param fieldName the record field to be indexed
     */
    public void addIndex(@Nonnull String recordType, @Nonnull String indexName, @Nonnull String fieldName) {
        addIndex(recordType, new Index(indexName, fieldName));
    }

    /**
     * Adds a new index on a single field.
     * @param recordType name of the record type
     * @param fieldName the record field to be indexed
     */
    public void addIndex(@Nonnull String recordType, @Nonnull String fieldName) {
        addIndex(recordType, recordType + "$" + fieldName, fieldName);
    }

    /**
     * Adds a new index that contains multiple record types.
     * If the list is null or empty, the resulting index will include all record types.
     * If the list has one element it will just be a normal single record type index.
     * @param recordTypes a list of record types that the index will include
     * @param index the index to be added
     */
    public void addMultiTypeIndex(@Nullable List<RecordTypeBuilder> recordTypes, @Nonnull Index index) {
        addIndexCommon(index);
        if (recordTypes == null || recordTypes.isEmpty()) {
            universalIndexes.put(index.getName(), index);
        } else if (recordTypes.size() == 1) {
            recordTypes.get(0).getIndexes().add(index);
        } else {
            for (RecordTypeBuilder recordType : recordTypes) {
                recordType.getMultiTypeIndexes().add(index);
            }
        }
    }

    /**
     * Adds a new index on all record types.
     * @param index the index to be added
     */
    public void addUniversalIndex(@Nonnull Index index) {
        addIndexCommon(index);
        universalIndexes.put(index.getName(), index);
    }

    public void removeIndex(@Nonnull String name) {
        Index index = indexes.remove(name);
        if (index == null) {
            throw new MetaDataException("No index named " + name + " defined");
        }
        for (RecordTypeBuilder recordType : recordTypes.values()) {
            recordType.getIndexes().remove(index);
            recordType.getMultiTypeIndexes().remove(index);
        }
        universalIndexes.remove(name);
        formerIndexes.add(new FormerIndex(index.getSubspaceKey(), index.getAddedVersion(), ++version, name));
    }

    public boolean isSplitLongRecords() {
        return splitLongRecords;
    }

    public void setSplitLongRecords(boolean splitLongRecords) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("No records added yet");
        }
        if (this.splitLongRecords != splitLongRecords) {
            version += 1;
            this.splitLongRecords = splitLongRecords;
        }
    }

    public boolean isStoreRecordVersions() {
        return storeRecordVersions;
    }

    public void setStoreRecordVersions(boolean storeRecordVersions) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("No records added yet");
        }
        if (this.storeRecordVersions != storeRecordVersions) {
            version += 1;
            this.storeRecordVersions = storeRecordVersions;
        }
    }

    /**
     * Get the record count key, if any.
     * @return the record count key of {@code null}
     * @deprecated use {@code COUNT} type indexes instead
     */
    @Nullable
    @Deprecated
    @API(API.Status.DEPRECATED)
    public KeyExpression getRecordCountKey() {
        return recordCountKey;
    }

    /**
     * Set the key used for maintaining record counts.
     * @param recordCountKey grouping key for counting
     * @deprecated use {@code COUNT} type indexes instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public void setRecordCountKey(KeyExpression recordCountKey) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("No records added yet");
        }
        if (!Objects.equals(this.recordCountKey, recordCountKey)) {
            version += 1;
            this.recordCountKey = recordCountKey;
        }
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("No records added yet");
        }
        this.version = version;
    }

    /**
     * If there is only one record type, get it.
     * @return the only type defined for this store.
     */
    @Nonnull
    public RecordTypeBuilder getOnlyRecordType() {
        if (recordTypes.size() != 1) {
            throw new MetaDataException("Must have exactly one record type defined.");
        }
        return recordTypes.values().iterator().next();
    }

    /**
     * Get the index registry used for validation.
     * @return the index maintainer registry
     */
    @Nonnull
    public IndexMaintainerRegistry getIndexMaintainerRegistry() {
        return indexMaintainerRegistry;
    }

    /**
     * Set the index registry used for validation.
     *
     * If a record store has a custom index maintainer registry, that same registry may need to be used to properly
     * validate the meta-data.
     * @param indexMaintainerRegistry the index maintainer registry
     * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.BaseBuilder#setIndexMaintainerRegistry
     */
    public void setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry) {
        this.indexMaintainerRegistry = indexMaintainerRegistry;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        if (recordMetaData == null || recordMetaData.getVersion() != version) {
            recordMetaData = build();
        }
        return recordMetaData;
    }

    /**
     * Build and validate meta-data.
     * @return new validated meta-data
     */
    @Nonnull
    public RecordMetaData build() {
        return build(true);
    }

    /**
     * Build and validate meta-data with specific index registry.
     * @param validate {@code true} to validate the new meta-data
     * @return new meta-data
     */
    @Nonnull
    public RecordMetaData build(boolean validate) {
        Map<String, RecordType> recordTypeBuilders = new HashMap<>();
        RecordMetaData metaData = new RecordMetaData(recordsDescriptor, unionDescriptor, unionFields, recordTypeBuilders,
                indexes, universalIndexes, formerIndexes,
                splitLongRecords, storeRecordVersions, version, recordCountKey);
        for (RecordTypeBuilder recordTypeBuilder : this.recordTypes.values()) {
            KeyExpression primaryKey = recordTypeBuilder.getPrimaryKey();
            if (primaryKey != null) {
                recordTypeBuilders.put(recordTypeBuilder.getName(), recordTypeBuilder.build(metaData));
                for (Index index : recordTypeBuilder.getIndexes()) {
                    index.setPrimaryKeyComponentPositions(buildPrimaryKeyComponentPositions(index.getRootExpression(), primaryKey));
                }
            } else {
                for (Index index : recordTypeBuilder.getIndexes()) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(KeyValueLogMessage.of("Created Index indexes a record type without a primary key",
                                "record_type", recordTypeBuilder.getName(),
                                "indexName", index.getName()));
                    }
                    indexes.remove(index.getName());
                }
            }
        }
        if (validate) {
            final MetaDataValidator validator = new MetaDataValidator(metaData, indexMaintainerRegistry);
            validator.validate();
        }
        return metaData;
    }

    // Note that there is no harm in this returning null for very complex overlaps; that just results in some duplication.
    @Nullable
    public static int[] buildPrimaryKeyComponentPositions(@Nonnull KeyExpression indexKey, @Nonnull KeyExpression primaryKey) {
        List<KeyExpression> indexKeys = indexKey.normalizeKeyForPositions();
        List<KeyExpression> primaryKeys = primaryKey.normalizeKeyForPositions();
        int[] positions = new int[primaryKeys.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = indexKeys.indexOf(primaryKeys.get(i));
        }
        if (Arrays.stream(positions).anyMatch(p -> p >= 0)) {
            return positions;
        } else {
            return null;
        }
    }

    /**
     * Exception thrown when meta-data cannot be loaded from serialized form.
     */
    @SuppressWarnings("serial")
    public static class MetaDataProtoDeserializationException extends MetaDataException {
        public MetaDataProtoDeserializationException(@Nullable Throwable cause) {
            super("Error converting from protobuf", cause);
        }
    }

}

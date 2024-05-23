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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.RecordTypeIndexesBuilder;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    private static final Descriptors.FileDescriptor[] emptyDependencyList = new Descriptors.FileDescriptor[0];
    public static final String DEFAULT_UNION_NAME = "RecordTypeUnion";

    @Nullable
    private Descriptors.FileDescriptor recordsDescriptor;
    @Nullable
    private Descriptors.Descriptor unionDescriptor;
    @Nullable
    private Descriptors.FileDescriptor localFileDescriptor;
    @Nonnull
    private final Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> unionFields;
    @Nonnull
    private final Map<String, RecordTypeBuilder> recordTypes;
    @Nonnull
    private final Map<String, SyntheticRecordTypeBuilder<?>> syntheticRecordTypes;
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
    @Nonnull
    private MetaDataEvolutionValidator evolutionValidator;
    @Nullable
    private KeyExpression recordCountKey;
    @Nullable
    private RecordMetaData recordMetaData;
    @Nonnull
    private final Map<String, Descriptors.FileDescriptor> explicitDependencies;
    private long subspaceKeyCounter = 0;
    private boolean usesSubspaceKeyCounter = false;

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
        evolutionValidator = MetaDataEvolutionValidator.getDefaultInstance();
        syntheticRecordTypes = new HashMap<>();
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
        for (RecordMetaDataProto.JoinedRecordType joinedProto : metaDataProto.getJoinedRecordTypesList()) {
            JoinedRecordTypeBuilder typeBuilder = new JoinedRecordTypeBuilder(joinedProto, this);
            syntheticRecordTypes.put(typeBuilder.getName(), typeBuilder);
        }
        for (RecordMetaDataProto.UnnestedRecordType unnestedProto : metaDataProto.getUnnestedRecordTypesList()) {
            UnnestedRecordTypeBuilder typeBuilder = new UnnestedRecordTypeBuilder(unnestedProto, this);
            syntheticRecordTypes.put(typeBuilder.getName(), typeBuilder);
        }

        for (RecordMetaDataProto.Index indexProto : metaDataProto.getIndexesList()) {
            List<RecordTypeBuilder> recordTypeBuilders = new ArrayList<>(indexProto.getRecordTypeCount());
            List<SyntheticRecordTypeBuilder<?>> syntheticRecordTypeBuilders = new ArrayList<>(indexProto.getRecordTypeCount());
            for (String recordTypeName : indexProto.getRecordTypeList()) {
                final RecordTypeBuilder recordTypeBuilder = recordTypes.get(recordTypeName);
                if (recordTypeBuilder != null) {
                    recordTypeBuilders.add(getRecordType(recordTypeName));
                } else {
                    final SyntheticRecordTypeBuilder<?> syntheticRecordTypeBuilder = syntheticRecordTypes.get(recordTypeName);
                    if (syntheticRecordTypeBuilder != null) {
                        syntheticRecordTypeBuilders.add(syntheticRecordTypeBuilder);
                    } else {
                        throwUnknownRecordType(recordTypeName, false);
                    }
                }
            }
            if (!syntheticRecordTypeBuilders.isEmpty()) {
                try {
                    Index index = new Index(indexProto);
                    addIndexCommon(index);
                    for (SyntheticRecordTypeBuilder<?> syntheticRecordTypeBuilder : syntheticRecordTypeBuilders) {
                        syntheticRecordTypeBuilder.getIndexes().add(index);
                    }
                } catch (KeyExpression.DeserializationException e) {
                    throw new MetaDataProtoDeserializationException(e);
                }
            } else {
                try {
                    addMultiTypeIndex(recordTypeBuilders, new Index(indexProto));
                } catch (KeyExpression.DeserializationException e) {
                    throw new MetaDataProtoDeserializationException(e);
                }
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
        loadSubspaceKeySettingsFromProto(metaDataProto);
        initRecordTypesAndUnion(processExtensionOptions);
        loadProtoExceptRecords(metaDataProto);
        processSchemaOptions(processExtensionOptions);

        // If a local file descriptor has been set, update records types (etc.) to use the local descriptor.
        if (localFileDescriptor != null) {
            Descriptors.Descriptor localUnionDescriptor = fetchLocalUnionDescriptor();
            evolutionValidator.validateUnion(unionDescriptor, localUnionDescriptor);
            updateUnionFieldsAndRecordTypesFromLocal(localUnionDescriptor);
            unionDescriptor = localUnionDescriptor;
        }
    }

    private void loadSubspaceKeySettingsFromProto(RecordMetaDataProto.MetaData metaDataProto) {
        if (metaDataProto.hasSubspaceKeyCounter() && !metaDataProto.getUsesSubspaceKeyCounter()) {
            throw new MetaDataProtoDeserializationException(new MetaDataException("subspaceKeyCounter is set but usesSubspaceKeyCounter is not set in the meta-data proto"));
        }
        if (metaDataProto.getUsesSubspaceKeyCounter() && !metaDataProto.hasSubspaceKeyCounter()) {
            throw new MetaDataProtoDeserializationException(new MetaDataException("usesSubspaceKeyCounter is set but subspaceKeyCounter is not set in the meta-data proto"));
        }
        if (!usesSubspaceKeyCounter()) {
            // Only read from the proto if user has not explicitly enabled it already.
            usesSubspaceKeyCounter = metaDataProto.getUsesSubspaceKeyCounter();
        }
        subspaceKeyCounter = Long.max(subspaceKeyCounter, metaDataProto.getSubspaceKeyCounter()); // User might have set the counter already.
    }

    private void loadFromFileDescriptor(@Nonnull Descriptors.FileDescriptor fileDescriptor,
                                        boolean processExtensionOptions) {
        recordsDescriptor = fileDescriptor;
        initRecordTypesAndUnion(processExtensionOptions);
        processSchemaOptions(processExtensionOptions);
    }

    private void initRecordTypesAndUnion(boolean processExtensionOptions) {
        if (recordsDescriptor == null) {
            // Should not happen as this method should only be called when a file descriptor has been set.
            throw new RecordCoreException("cannot initiate records from null file descriptor");
        }
        unionDescriptor = fetchUnionDescriptor(recordsDescriptor);
        validateRecords(recordsDescriptor, unionDescriptor);
        fillUnionFields(processExtensionOptions);
    }

    @Nonnull
    private static Descriptors.Descriptor fetchUnionDescriptor(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
        @Nullable Descriptors.Descriptor union = null;
        for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null && recordTypeOptions.hasUsage()) {
                switch (recordTypeOptions.getUsage()) {
                    case UNION:
                        if (union != null) {
                            throw new MetaDataException("Only one union descriptor is allowed");
                        }
                        union = descriptor;
                        continue;
                    case NESTED:
                        if (DEFAULT_UNION_NAME.equals(descriptor.getName())) {
                            throw new MetaDataException("Message type " + DEFAULT_UNION_NAME + " cannot have NESTED usage");
                        }
                        continue;
                    case RECORD:
                    case UNSET:
                    default:
                        break;
                }
            }
            if (DEFAULT_UNION_NAME.equals(descriptor.getName())) {
                if (union != null) {
                    throw new MetaDataException("Only one union descriptor is allowed");
                }
                union = descriptor;
                continue;
            }
        }
        if (union == null) {
            throw new MetaDataException("Union descriptor is required");
        }
        return union;
    }

    @Nonnull
    private static Map<String, Descriptors.FileDescriptor> initGeneratedDependencies(@Nonnull Map<String, DescriptorProtos.FileDescriptorProto> protoDependencies) {
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
        if (localFileDescriptor != null) {
            throw new MetaDataException("Cannot set records from file descriptor when local descriptor is specified.");
        }
        if (!explicitDependencies.isEmpty()) {
            throw new MetaDataException("Cannot set records from file descriptor when explicit dependencies are specified.");
        }
        loadFromFileDescriptor(fileDescriptor, processExtensionOptions);
        return this;
    }

    /**
     * Update the records descriptor of the record meta-data.
     *
     * <p>
     * This involves adding new record types and updating descriptors for the existing record types and union fields.
     * By contract, the extension options will be processed for all of the new record types and will not be processed
     * for any of the old record types. Also, it is not allowed to call this method when the local file descriptor is set.
     * </p>
     *
     * <p>
     * See {@link #updateRecords(Descriptors.FileDescriptor, boolean)} for more information.
     * </p>
     *
     * @param recordsDescriptor the new record descriptor
     */
    public void updateRecords(@Nonnull Descriptors.FileDescriptor recordsDescriptor) {
        updateRecords(recordsDescriptor, true);
    }

    /**
     * Update the records descriptor of the record meta-data.
     *
     * <p>
     * This adds any new record types and updates the descriptors for existing record types.
     * By contract, the extension options will not be processed for the old record types. If {@code processExtensionOptions}
     * is set, extension options will be processed only for the new record types. Also, this method may not be called
     * when {@code localFileDescriptor} is set. This method bumps the meta-data version and sets the {@linkplain RecordType#getSinceVersion() since version}
     * for the new record types (and their indexes).
     * </p>
     *
     * <p>
     * To avoid accidental changes, this method only updates the record types (i.e., updates the message
     * descriptors of the old record types and adds new record types with their primary keys and indexes). This method
     * does not process schema options. To update the schema options, use {@link #setSplitLongRecords(boolean)} and
     * {@link #setStoreRecordVersions(boolean)}.
     * </p>
     *
     * @param newRecordsDescriptor the new record descriptor
     * @param processExtensionOptions whether to add primary keys and indexes using the extensions in the protobuf (only for the new record types)
     */
    public void updateRecords(@Nonnull Descriptors.FileDescriptor newRecordsDescriptor, boolean processExtensionOptions) {
        if (recordsDescriptor == null) {
            throw new MetaDataException("Records descriptor is not set yet");
        }
        if (localFileDescriptor != null) {
            throw new MetaDataException("Updating the records descriptor is not allowed when the local file descriptor is set");
        }
        if (unionDescriptor == null) {
            throw new RecordCoreException("cannot update record types as no previous union descriptor has been set");
        }
        Descriptors.Descriptor newUnionDescriptor = fetchUnionDescriptor(newRecordsDescriptor);
        validateRecords(newRecordsDescriptor, newUnionDescriptor);
        evolutionValidator.validateUnion(unionDescriptor, newUnionDescriptor);
        version++; // Bump the meta-data version
        updateUnionFieldsAndRecordTypes(newUnionDescriptor, processExtensionOptions);
        recordsDescriptor = newRecordsDescriptor;
        unionDescriptor = newUnionDescriptor;
    }

    /**
     * Sets the local file descriptor. A local meta-data file descriptor may contain newer versions of record types or indexes.
     *
     * <p>
     * This method is handy when two versions of the record meta-data are compatible (i.e., they follow the
     * <a href="https://foundationdb.github.io/fdb-record-layer/SchemaEvolution.html">Record Layer guidelines on schema evolution</a>),
     * but their descriptors are not equal, e.g., a statically-generated proto and its serialized version stored
     * in a meta-data store (i.e., {@link com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore}). A
     * record store created using the meta-data store may not be able to store a record created
     * by the statically-generated proto file because the meta-data and record have mismatched descriptors. Using this method,
     * the meta-data can use the same version of the descriptor as the record.
     * </p>
     *
     * <p>
     * This should only be used when the records descriptor is set through a meta-data Protobuf message (i.e, it is followed by a call
     * to {@link #setRecords(RecordMetaDataProto.MetaData)} or {@link #setRecords(RecordMetaDataProto.MetaData, boolean)}).
     * This will not work if the records are set using a file descriptor. Note also that once the local file descriptor is
     * set, the {@link RecordMetaData} object that is produced from this builder may differ from the original Protobuf definition
     * in ways that make serializing the meta-data back to Protobuf unsafe. As a result, calling {@link RecordMetaData#toProto() toProto()}
     * on the produced {@code RecordMetaData} is disallowed.
     * </p>
     *
     * <p>
     * To verify that the file descriptor supplied here and the file descriptor included in the meta-data proto are
     * compatible, the two descriptors are compared using a {@link MetaDataEvolutionValidator}. The user may provide
     * their own validator by calling {@link #setEvolutionValidator(MetaDataEvolutionValidator)}. By default, this
     * will use that class's {@linkplain MetaDataEvolutionValidator#getDefaultInstance() default instance}.
     * </p>
     *
     * @param localFileDescriptor a file descriptor that contains updated record types
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder setLocalFileDescriptor(@Nonnull Descriptors.FileDescriptor localFileDescriptor) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set.");
        }
        this.localFileDescriptor = localFileDescriptor;
        return this;
    }

    @Nonnull
    private Descriptors.Descriptor buildSyntheticUnion(@Nonnull Descriptors.FileDescriptor parentFileDescriptor) {
        if (unionDescriptor == null) {
            throw new RecordCoreException("cannot build a synthetic union descriptor as no prior existing union descriptor has been set");
        }
        DescriptorProtos.FileDescriptorProto.Builder builder = DescriptorProtos.FileDescriptorProto.newBuilder();
        builder.setName("_synthetic_" + parentFileDescriptor.getName());
        builder.addMessageType(MetaDataProtoEditor.createSyntheticUnion(parentFileDescriptor, unionDescriptor));
        builder.addDependency(parentFileDescriptor.getName());
        return fetchUnionDescriptor(buildFileDescriptor(builder.build(), new Descriptors.FileDescriptor[]{parentFileDescriptor}));
    }

    @Nonnull
    private Descriptors.Descriptor fetchLocalUnionDescriptor() {
        if (localFileDescriptor == null) {
            // Should not be reached as caller should guard this call on checking for the local file descriptor
            throw new RecordCoreException("cannot fetch local union descriptor as no local file is set");
        }
        Descriptors.Descriptor localUnionDescriptor;
        if (MetaDataProtoEditor.hasUnion(localFileDescriptor)) {
            localUnionDescriptor = fetchUnionDescriptor(localFileDescriptor);
        } else {
            // The local file descriptor does not have a union. Synthesize it.
            localUnionDescriptor = buildSyntheticUnion(localFileDescriptor);
        }
        validateRecords(localFileDescriptor, localUnionDescriptor);
        return localUnionDescriptor;
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
                throw new MetaDataException("Dependency not found").addLogInfo(LogMessageKeys.VALUE, key);
            }
        }
        return dependencies;
    }

    private static void validateRecords(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull Descriptors.Descriptor unionDescriptor) {
        validateDataTypes(fileDescriptor);
        validateUnion(fileDescriptor, unionDescriptor);
    }

    private static void validateDataTypes(@Nonnull Descriptors.FileDescriptor fileDescriptor) {
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

    private static void validateUnion(@Nonnull Descriptors.FileDescriptor fileDescriptor, @Nonnull Descriptors.Descriptor unionDescriptor) {
        for (Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
            // Only message types allowed.
            if (unionField.getType() != Descriptors.FieldDescriptor.Type.MESSAGE) {
                throw new MetaDataException("Union field " + unionField.getName() +
                                            " is not a message");
            }
            // No repeating fields.
            if (unionField.isRepeated()) {
                throw new MetaDataException("Union field " + unionField.getName() +
                                            " should not be repeated");
            }
            Descriptors.Descriptor descriptor = unionField.getMessageType();
            // RecordTypeUnion is reserved for union descriptor and cannot appear as a union fields
            if (DEFAULT_UNION_NAME.equals(descriptor.getName())) {
                throw new MetaDataException("Union message type " + descriptor.getName()
                                            + " cannot be a union field.");
            }

            // All union fields must be RECORD (i.e., they cannot be NESTED/UNIONs). The same rule applies to imported union fields too.
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null &&
                    recordTypeOptions.hasUsage() &&
                    recordTypeOptions.getUsage() != RecordMetaDataOptionsProto.RecordTypeOptions.Usage.RECORD) {
                throw new MetaDataException("Union field " + unionField.getName() +
                                            " has type " + descriptor.getName() +
                                            " which is not a record");
            }
        }

        // All RECORD message types defined in this proto must be present in the union.
        for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null && recordTypeOptions.hasUsage()) {
                switch (recordTypeOptions.getUsage()) {
                    case RECORD:
                        if (DEFAULT_UNION_NAME.equals(descriptor.getName())) {
                            if (unionHasMessageType(unionDescriptor, descriptor)) {
                                throw new MetaDataException("Union message type " + descriptor.getName()
                                                            + " cannot be a union field.");
                            }
                        } else if (!unionHasMessageType(unionDescriptor, descriptor)) {
                            throw new MetaDataException("Record message type " + descriptor.getName()
                                                        + " must be a union field.");
                        }
                        break;
                    case UNION:
                        // Already checked above that none of the union fields is UNION.
                    case NESTED:
                        // Already checked above that none of the union fields is NESTED.
                    case UNSET:
                        // Unset message types can appear in either union fields or as nested messages. The only possible issue
                        // is that the message type's name might equal DEFAULT_UNION_NAME. This is already checked above.
                    default:
                        break;
                }
            }
        }
    }

    private static boolean unionHasMessageType(@Nonnull Descriptors.Descriptor unionDescriptor, @Nonnull Descriptors.Descriptor descriptor) {
        return unionDescriptor.getFields().stream().anyMatch(field -> descriptor == field.getMessageType());
    }


    private void updateUnionFieldsAndRecordTypes(@Nonnull Descriptors.Descriptor union, boolean processExtensionOptions) {
        final Map<String, RecordTypeBuilder> oldRecordTypes = ImmutableMap.copyOf(recordTypes);
        recordTypes.clear();
        unionFields.clear();
        for (Descriptors.FieldDescriptor unionField : union.getFields()) {
            Descriptors.Descriptor newDescriptor = unionField.getMessageType();
            Descriptors.Descriptor oldDescriptor = findOldDescriptor(unionField, union);
            if (unionFields.containsKey(newDescriptor)) {
                if (!recordTypes.containsKey(newDescriptor.getName())) {
                    // Union field was seen before but the record type is unknown? This must not happen.
                    throw new MetaDataException("Unknown record type for union field " + unionField.getName());
                }
                // For existing record types, the preferred field is the last one, except if there is one whose name matches.
                remapUnionField(newDescriptor, unionField);
            } else if (oldDescriptor == null) {
                // New field and record type.
                RecordTypeBuilder recordType = processRecordType(unionField, processExtensionOptions);
                if (recordType.getSinceVersion() != null && recordType.getSinceVersion() != version) {
                    throw new MetaDataException("Record type since version does not match meta-data version")
                            .addLogInfo(LogMessageKeys.META_DATA_VERSION, version)
                            .addLogInfo("since_version", recordType.getSinceVersion())
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, recordType.getName());
                } else {
                    recordType.setSinceVersion(version);
                }
                unionFields.put(newDescriptor, unionField);
            } else {
                updateRecordType(oldRecordTypes, oldDescriptor, newDescriptor);
                unionFields.put(newDescriptor, unionField);
            }
        }
    }

    private void updateUnionFieldsAndRecordTypesFromLocal(@Nonnull Descriptors.Descriptor union) {
        final Map<String, RecordTypeBuilder> oldRecordTypes = ImmutableMap.copyOf(recordTypes);
        final Map<Descriptors.Descriptor, Descriptors.FieldDescriptor> oldUnionFields = ImmutableMap.copyOf(unionFields);
        recordTypes.clear();
        unionFields.clear();
        for (Descriptors.FieldDescriptor unionField : union.getFields()) {
            Descriptors.Descriptor newDescriptor = unionField.getMessageType();
            Descriptors.Descriptor oldDescriptor = findOldDescriptor(unionField, union);
            if (oldDescriptor == null) {
                // If updating from a local union descriptor, do not process any new types.
                continue;
            }
            if (unionFields.containsKey(newDescriptor)) {
                if (!recordTypes.containsKey(newDescriptor.getName())) {
                    // Union field was seen before but the record type is unknown? This must not happen.
                    throw new MetaDataException("Unknown record type for union field " + unionField.getName());
                }
                // When pulling from a local file descriptor, the preferred field is the one that has the same
                // position as the field in the old union descriptor (so that the byte representation is the same).
                Descriptors.FieldDescriptor oldUnionField = Verify.verifyNotNull(oldUnionFields.get(oldDescriptor));
                if (unionField.getNumber() == oldUnionField.getNumber()) {
                    unionFields.put(newDescriptor, unionField);
                }
            } else {
                updateRecordType(oldRecordTypes, oldDescriptor, newDescriptor);
                unionFields.put(newDescriptor, unionField);
            }
        }
    }

    @Nullable
    private static Descriptors.Descriptor getCorrespondingFieldType(@Nonnull Descriptors.Descriptor descriptor, @Nonnull Descriptors.FieldDescriptor field) {
        Descriptors.FieldDescriptor correspondingField = descriptor.findFieldByNumber(field.getNumber());
        if (correspondingField != null) {
            return correspondingField.getMessageType();
        }
        return null;
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private Descriptors.Descriptor findOldDescriptor(@Nonnull Descriptors.FieldDescriptor newUnionField, @Nonnull Descriptors.Descriptor newUnion) {
        if (unionDescriptor == null) {
            throw new RecordCoreException("cannot get field from union as it has not been set");
        }
        // If there is a corresponding field in the old union, use that field.
        Descriptors.Descriptor correspondingFieldType = getCorrespondingFieldType(unionDescriptor, newUnionField);
        if (correspondingFieldType != null) {
            return correspondingFieldType;
        }
        // Look for a field in the new union of the same type as this one and look for a matching field in the old union
        final Descriptors.Descriptor newDescriptor = newUnionField.getMessageType();
        for (Descriptors.FieldDescriptor otherNewUnionField : newUnion.getFields()) {
            if (otherNewUnionField.getMessageType() == newDescriptor) {
                final Descriptors.Descriptor otherCorrespondingFieldType = getCorrespondingFieldType(unionDescriptor, otherNewUnionField);
                if (otherCorrespondingFieldType != null) {
                    return otherCorrespondingFieldType;
                }
            }
        }
        return null;
    }

    private void updateRecordType(@Nonnull Map<String, RecordTypeBuilder> oldRecordTypes,
                                  @Nonnull Descriptors.Descriptor oldDescriptor,
                                  @Nonnull Descriptors.Descriptor newDescriptor) {
        // Create a new record type based off the old one
        RecordTypeBuilder oldRecordType = oldRecordTypes.get(oldDescriptor.getName());
        RecordTypeBuilder newRecordType = new RecordTypeBuilder(newDescriptor, oldRecordType);
        recordTypes.put(newRecordType.getName(), newRecordType); // update the record type builder
    }

    private void fillUnionFields(boolean processExtensionOptions) {
        if (unionDescriptor == null) {
            throw new RecordCoreException("cannot fill union fiends as no union descriptor has been set");
        }
        if (!unionFields.isEmpty()) {
            throw new RecordCoreException("cannot set union fields twice");
        }
        for (Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
            Descriptors.Descriptor descriptor = unionField.getMessageType();
            if (!unionFields.containsKey(descriptor)) {
                processRecordType(unionField, processExtensionOptions);
                unionFields.put(descriptor, unionField);
            } else {
                // The preferred field is the last one, except if there is one whose name matches.
                remapUnionField(descriptor, unionField);
            }
        }
    }

    private void remapUnionField(@Nonnull Descriptors.Descriptor descriptor, @Nonnull Descriptors.FieldDescriptor unionField) {
        unionFields.compute(descriptor, (d, f) -> {
            // Prefer the field that has the name in the right format or, if neither do, the one with the larger field number.
            final String canonicalName = "_" + d.getName();
            if (f == null || unionField.getName().equals(canonicalName) || !f.getName().equals(canonicalName) && unionField.getNumber() > f.getNumber()) {
                return unionField;
            } else {
                return f;
            }
        });
    }

    @Nonnull
    private RecordTypeBuilder processRecordType(@Nonnull Descriptors.FieldDescriptor unionField, boolean processExtensionOptions) {
        Descriptors.Descriptor descriptor = unionField.getMessageType();
        RecordTypeBuilder recordType = new RecordTypeBuilder(descriptor);
        if (recordTypes.putIfAbsent(recordType.getName(), recordType) != null) {
            throw new MetaDataException("There is already a record type named " + recordType.getName());
        }
        if (processExtensionOptions) {
            RecordMetaDataOptionsProto.RecordTypeOptions recordTypeOptions = descriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.record);
            if (recordTypeOptions != null && recordTypeOptions.hasSinceVersion()) {
                recordType.setSinceVersion(recordTypeOptions.getSinceVersion());
            }
            if (recordTypeOptions != null && recordTypeOptions.hasRecordTypeKey()) {
                recordType.setRecordTypeKey(LiteralKeyExpression.fromProto(recordTypeOptions.getRecordTypeKey()).getValue());
            }
            protoFieldOptions(recordType);
        }
        return recordType;
    }

    private void protoFieldOptions(RecordTypeBuilder recordType) {
        // Add indexes from custom options.
        for (Descriptors.FieldDescriptor fieldDescriptor : recordType.getDescriptor().getFields()) {
            RecordMetaDataOptionsProto.FieldOptions fieldOptions = fieldDescriptor.getOptions()
                    .getExtension(RecordMetaDataOptionsProto.field);
            if (fieldOptions != null) {
                protoFieldOptions(recordType, fieldDescriptor, fieldOptions);
            }
        }
    }

    @SuppressWarnings("deprecation")
    private void protoFieldOptions(RecordTypeBuilder recordType, Descriptors.FieldDescriptor fieldDescriptor, RecordMetaDataOptionsProto.FieldOptions fieldOptions) {
        Descriptors.Descriptor descriptor = recordType.getDescriptor();
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
            if (IndexTypes.RANK.equals(type)) {
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
            throwUnknownRecordType(name, false);
        }
        return recordType;
    }

    private void throwUnknownRecordType(final @Nonnull String name, boolean isSynthetic) {
        throw new MetaDataException("Unknown " + (isSynthetic ? "Synthetic " : "") + "record type " + name);
    }


    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    @SuppressWarnings("squid:S1452")
    public SyntheticRecordTypeBuilder<?> getSyntheticRecordType(@Nonnull String name) {
        SyntheticRecordTypeBuilder<?> recordType = syntheticRecordTypes.get(name);
        if (recordType == null) {
            throwUnknownRecordType(name, true);
        }
        return recordType;
    }

    /**
     * Get the next record type key for a synthetic record type.
     *
     * These keys are negative, unlike stored record types which are initially positive.
     * This isn't strictly speaking necessary, but simplifies debugging.
     * @return a new unique record type key
     */
    @Nonnull
    private Long getNextRecordTypeKey() {
        long minKey = 0;
        for (SyntheticRecordTypeBuilder<?> syntheticRecordType : syntheticRecordTypes.values()) {
            if (syntheticRecordType.getRecordTypeKey() instanceof Number) {
                long key = ((Number)syntheticRecordType.getRecordTypeKey()).longValue();
                if (minKey > key) {
                    minKey = key;
                }
            }
        }
        return minKey - 1;
    }

    /**
     * Add a new joined record type.
     * @param name the name of the new record type
     * @return a new uninitialized joined record type
     */
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public JoinedRecordTypeBuilder addJoinedRecordType(@Nonnull String name) {
        if (recordTypes.containsKey(name)) {
            throw new MetaDataException("There is already a record type named " + name);
        }
        if (syntheticRecordTypes.containsKey(name)) {
            throw new MetaDataException("There is already a synthetic record type named " + name);
        }
        JoinedRecordTypeBuilder recordType = new JoinedRecordTypeBuilder(name, getNextRecordTypeKey(), this);
        syntheticRecordTypes.put(name, recordType);
        return recordType;
    }

    /**
     * Add a new {@link UnnestedRecordTypeBuilder}.
     * @param name the name of the new record type
     * @return a new uninitialized unnested record type
     */
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public UnnestedRecordTypeBuilder addUnnestedRecordType(@Nonnull String name) {
        if (recordTypes.containsKey(name)) {
            throw new MetaDataException("There is already a record type named " + name);
        }
        if (syntheticRecordTypes.containsKey(name)) {
            throw new MetaDataException("There is already a synthetic record type named " + name);
        }
        UnnestedRecordTypeBuilder unnestedRecordTypeBuilder = new UnnestedRecordTypeBuilder(name, getNextRecordTypeKey(), this);
        syntheticRecordTypes.put(name, unnestedRecordTypeBuilder);
        return unnestedRecordTypeBuilder;
    }

    /**
     * Get a record type or synthetic record type by name for use with {@link #addIndex}.
     * @param name the name of the record type
     * @return the possibly synthetic record type
     */
    public RecordTypeIndexesBuilder getIndexableRecordType(@Nonnull String name) {
        RecordTypeIndexesBuilder recordType = recordTypes.get(name);
        if (recordType == null) {
            recordType = syntheticRecordTypes.get(name);
        }
        if (recordType == null) {
            throwUnknownRecordType(name, false);
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
        if (usesSubspaceKeyCounter && !index.hasExplicitSubspaceKey()) {
            index.setSubspaceKey(++subspaceKeyCounter);
        }
        indexes.put(index.getName(), index);
    }

    /**
     * Adds a new index. This index can either be a universal index or an index for
     * a single record type.
     * @param recordType if null this index will exist for all record types
     * @param index the index to be added
     */
    public void addIndex(@Nullable RecordTypeIndexesBuilder recordType, @Nonnull Index index) {
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
        addIndex(getIndexableRecordType(recordType), index);
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
    public void addMultiTypeIndex(@Nullable List<? extends RecordTypeIndexesBuilder> recordTypes, @Nonnull Index index) {
        addIndexCommon(index);
        if (recordTypes == null || recordTypes.isEmpty()) {
            universalIndexes.put(index.getName(), index);
        } else if (recordTypes.size() == 1) {
            recordTypes.get(0).getIndexes().add(index);
        } else {
            for (RecordTypeIndexesBuilder recordType : recordTypes) {
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

    public void addFormerIndex(@Nonnull FormerIndex formerIndex) {
        formerIndexes.add(formerIndex);
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
    // Deprecated for new usage, but this can't really be removed because old meta-data might include it.
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
    // Deprecated for new usage, but this can't really be removed because old meta-data might include it.
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
     * Enable counter-based subspace keys assignment.
     *
     * <p>
     * If enabled, index subspace keys will be set using a counter instead of defaulting to the indexes' names. This
     * must be called prior to setting the records descriptor (for example {@link #setRecords(Descriptors.FileDescriptor)}).
     * </p>
     *
     * <p>
     * Existing clients should be careful about enabling this feature. The name of an index is the default
     * value of its subspace key when counter-based subspace keys are disabled. If a subspace key was not set
     * explicitly before, enabling the counter-based scheme will change the index's subspace key. Note that
     * it is important that the subspace key of an index that has data does not change.
     * See {@link Index#setSubspaceKey(Object)} for more details.
     * </p>
     *
     * @return this builder
     */
    @Nonnull
    public RecordMetaDataBuilder enableCounterBasedSubspaceKeys() {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records descriptor has already been set.");
        }
        this.usesSubspaceKeyCounter = true;
        return this;
    }

    /**
     * Checks if counter-based subspace key assignment is used.
     * @return {@code true} if the subspace key counter is used
     */
    public boolean usesSubspaceKeyCounter() {
        return usesSubspaceKeyCounter;
    }

    /**
     * Get the current value of the index subspace key counter. If it is not enabled, the value will be 0.
     * @return the current value of the index subspace key counter
     * @see #enableCounterBasedSubspaceKeys()
     */
    public long getSubspaceKeyCounter() {
        return subspaceKeyCounter;
    }

    /**
     * Set the initial value of the subspace key counter. This method can be handy when users want to assign
     * subspace keys using a counter, but their indexes already have subspace keys that may conflict with the
     * counter-based assignment.
     *
     * <p>
     * Note that the new counter must be greater than the current value. Also, users must first enable this feature by
     * calling {@link #enableCounterBasedSubspaceKeys()} before updating the counter value.
     * </p>
     *
     * @param subspaceKeyCounter the new value
     * @return this builder
     * @see #enableCounterBasedSubspaceKeys()
     */
    @Nonnull
    public RecordMetaDataBuilder setSubspaceKeyCounter(long subspaceKeyCounter) {
        if (!usesSubspaceKeyCounter()) {
            throw new MetaDataException("Counter-based subspace keys not enabled");
        }
        if (subspaceKeyCounter <= this.subspaceKeyCounter) {
            throw new MetaDataException("Subspace key counter must be set to a value greater than its current value")
                    .addLogInfo(LogMessageKeys.EXPECTED, "greater than " + this.subspaceKeyCounter)
                    .addLogInfo(LogMessageKeys.ACTUAL, subspaceKeyCounter);
        }
        this.subspaceKeyCounter = subspaceKeyCounter;
        return this;
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

    /**
     * Get the validator used to compare the local file descriptor to the descriptor included
     * in the meta-data proto. By default, this instance is set to the {@link MetaDataEvolutionValidator}'s
     * {@linkplain MetaDataEvolutionValidator#getDefaultInstance() default instance}, but the
     * user may provide their own through {@link #setEvolutionValidator(MetaDataEvolutionValidator)}
     * if they want to tweak certain validator options.
     *
     * @return the validator used to check the local file descriptor against the one in the meta-data proto
     * @see #setLocalFileDescriptor(Descriptors.FileDescriptor)
     * @see MetaDataEvolutionValidator
     */
    @Nonnull
    public MetaDataEvolutionValidator getEvolutionValidator() {
        return evolutionValidator;
    }

    /**
     * Set the validator used to compare the local file descriptor to the descriptor included
     * in the meta-data proto. As this validator is used only to check whether the local file descriptor
     * is compatible with the records descriptor set in {@link #setRecords(RecordMetaDataProto.MetaData) setRecords()}
     * through the meta-data proto, this method must be called before {@code setRecords()}.
     *
     * @param evolutionValidator the validator used to check the local file descriptor against the one in the meta-data proto
     * @return this builder
     * @see #setLocalFileDescriptor(Descriptors.FileDescriptor)
     * @see MetaDataEvolutionValidator
     */
    @Nonnull
    public RecordMetaDataBuilder setEvolutionValidator(@Nonnull MetaDataEvolutionValidator evolutionValidator) {
        if (recordsDescriptor != null) {
            throw new MetaDataException("Records already set.");
        }
        this.evolutionValidator = evolutionValidator;
        return this;
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
        Map<String, RecordType> builtRecordTypes = Maps.newHashMapWithExpectedSize(recordTypes.size());
        Map<String, SyntheticRecordType<?>> builtSyntheticRecordTypes = Maps.newHashMapWithExpectedSize(syntheticRecordTypes.size());
        Map<Object, SyntheticRecordType<?>> recordTypeKeyToSyntheticRecordTypeMap = Maps.newHashMapWithExpectedSize(syntheticRecordTypes.size());
        RecordMetaData metaData = new RecordMetaData(recordsDescriptor, getUnionDescriptor(), unionFields,
                builtRecordTypes, builtSyntheticRecordTypes, recordTypeKeyToSyntheticRecordTypeMap,
                indexes, universalIndexes, formerIndexes,
                splitLongRecords, storeRecordVersions, version, subspaceKeyCounter, usesSubspaceKeyCounter, recordCountKey, localFileDescriptor != null);
        for (RecordTypeBuilder recordTypeBuilder : recordTypes.values()) {
            KeyExpression primaryKey = recordTypeBuilder.getPrimaryKey();
            if (primaryKey != null) {
                builtRecordTypes.put(recordTypeBuilder.getName(), recordTypeBuilder.build(metaData));
                for (Index index : recordTypeBuilder.getIndexes()) {
                    index.setPrimaryKeyComponentPositions(buildPrimaryKeyComponentPositions(index.getRootExpression(), primaryKey));
                }
            } else {
                throw new MetaDataException("Record type " +
                                            recordTypeBuilder.getName() +
                                            " must have a primary key");
            }
        }
        if (!syntheticRecordTypes.isEmpty()) {
            DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
            fileBuilder.setName("_synthetic");
            Set<Descriptors.FileDescriptor> typeDescriptorSources = new LinkedHashSet<>(); // for stable iteration order
            syntheticRecordTypes.values().forEach(recordTypeBuilder -> recordTypeBuilder.buildDescriptor(fileBuilder, typeDescriptorSources));
            typeDescriptorSources.forEach(source -> fileBuilder.addDependency(source.getName()));
            final Descriptors.FileDescriptor fileDescriptor;
            try {
                final Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[typeDescriptorSources.size()];
                typeDescriptorSources.toArray(dependencies);
                fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), dependencies);
            } catch (Descriptors.DescriptorValidationException ex) {
                throw new MetaDataException("Could not build synthesized file descriptor", ex);
            }
            for (SyntheticRecordTypeBuilder<?> recordTypeBuilder : syntheticRecordTypes.values()) {
                final SyntheticRecordType<?> syntheticType = recordTypeBuilder.build(metaData, fileDescriptor);
                builtSyntheticRecordTypes.put(recordTypeBuilder.getName(), syntheticType);
                recordTypeKeyToSyntheticRecordTypeMap.put(syntheticType.getRecordTypeKey(), syntheticType);
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

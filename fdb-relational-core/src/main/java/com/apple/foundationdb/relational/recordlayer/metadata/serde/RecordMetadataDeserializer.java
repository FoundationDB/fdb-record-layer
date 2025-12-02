/*
 * RecordMetadataDeserializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata.serde;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.record.query.plan.cascades.UserDefinedMacroFunction;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerView;
import com.apple.foundationdb.relational.recordlayer.query.LogicalOperator;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This deserializes a {@link RecordMetaData} object into a corresponding {@link RecordLayerSchemaTemplate}.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordMetadataDeserializer {

    @Nonnull
    private final RecordMetaData recordMetaData;

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder builder;

    public RecordMetadataDeserializer(@Nonnull final RecordMetaData recordMetaData) {
        this.recordMetaData = recordMetaData;
        builder = deserializeRecordMetaData();
    }

    @Nonnull
    public RecordLayerSchemaTemplate getSchemaTemplate(@Nonnull final String schemaTemplateName, int version) {
        return builder.setName(schemaTemplateName).setVersion(version).build();
    }

    @Nonnull
    private RecordLayerSchemaTemplate.Builder deserializeRecordMetaData() {
        // iterate _only_ over the record types registered in the union descriptor to avoid potentially-expensive
        // deserialization of other descriptors that can never be used by the user.
        final var unionDescriptor = recordMetaData.getUnionDescriptor();

        final List<Descriptors.FieldDescriptor> registeredTypes = unionDescriptor.getFields();
        final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder = RecordLayerSchemaTemplate.newBuilder()
                .setStoreRowVersions(recordMetaData.isStoreRecordVersions())
                .setEnableLongRows(recordMetaData.isSplitLongRecords())
                .setIntermingleTables(!recordMetaData.primaryKeyHasRecordTypePrefix());
        final Map<String, RecordLayerTable.Builder> nameToTableBuilder = new HashMap<>();
        for (final Descriptors.FieldDescriptor registeredType : registeredTypes) {
            switch (registeredType.getType()) {
                case MESSAGE:
                    final String storageName = registeredType.getMessageType().getName();
                    final String userName = ProtoUtils.toUserIdentifier(storageName);
                    if (!nameToTableBuilder.containsKey(userName)) {
                        nameToTableBuilder.put(userName, generateTableBuilder(userName, storageName));
                    }
                    nameToTableBuilder.get(userName).addGeneration(registeredType.getNumber(), registeredType.getOptions());
                    break;
                case ENUM:
                    // todo (yhatem) this is temporary, we rely on rec layer type system to deserialize protobuf descriptors.
                    final var recordLayerType = Type.Enum.fromDescriptorPreservingNames(false, registeredType.getEnumType());
                    schemaTemplateBuilder.addAuxiliaryType((DataType.Named) DataTypeUtils.toRelationalType(recordLayerType));
                    break;
                default:
                    Assert.failUnchecked(String.format(Locale.ROOT, "Unexpected type '%s' found in union descriptor!", registeredType.getType()));
                    break;
            }
        }
        nameToTableBuilder.values().stream().map(RecordLayerTable.Builder::build).forEach(schemaTemplateBuilder::addTable);
        if (!recordMetaData.getUserDefinedFunctionMap().isEmpty()) {
            final var metadataProvider = Suppliers.memoize(schemaTemplateBuilder::build);
            // TODO: topsort deps of functions.
            for (final var function : recordMetaData.getUserDefinedFunctionMap().entrySet()) {
                if (function.getValue() instanceof RawSqlFunction) {
                    schemaTemplateBuilder.addInvokedRoutine(generateInvokedRoutineBuilder(metadataProvider, function.getKey(),
                            Assert.castUnchecked(function.getValue(), RawSqlFunction.class).getDefinition()).build());
                } else if (function.getValue() instanceof UserDefinedMacroFunction) {
                    schemaTemplateBuilder.addInvokedRoutine(generateInvokedRoutineBuilder(function.getKey(), function.getValue().toString(),
                            Assert.castUnchecked(function.getValue(), UserDefinedMacroFunction.class)).build());
                }
            }
        }
        if (!recordMetaData.getViewMap().isEmpty()) {
            final var metadataProvider = Suppliers.memoize(schemaTemplateBuilder::build);
            for (final var view : recordMetaData.getViewMap().entrySet()) {
                schemaTemplateBuilder.addView(generateViewBuilder(metadataProvider, view.getKey(), view.getValue().getDefinition()).build());
            }
        }
        schemaTemplateBuilder.setCachedMetadata(getRecordMetaData());
        return schemaTemplateBuilder;
    }

    @Nonnull
    private RecordLayerTable.Builder generateTableBuilder(@Nonnull final String userName, @Nonnull final String storageName) {
        final RecordType recordType = recordMetaData.getRecordType(storageName);

        // todo (yhatem) we rely on the record type for deserialization from ProtoBuf for now, later on
        //      we will avoid this step by having our own deserializers.
        var recordLayerType = Type.Record.fromDescriptor(recordType.getDescriptor()).withNameAndStorageName(userName, storageName);
        // todo (yhatem) this is hacky and must be cleaned up. We need to understand the actually field types so we can take decisions
        // on higher level based on these types (wave3).
        if (recordLayerType.getFields().stream().anyMatch(f -> f.getFieldType().isRecord())) {
            ImmutableList.Builder<Type.Record.Field> newFields = ImmutableList.builder();
            for (int i = 0; i < recordLayerType.getFields().size(); i++) {
                final var protoField = recordType.getDescriptor().getFields().get(i);
                final var field = recordLayerType.getField(i);
                final Type fieldtype = field.getFieldType();
                if (fieldtype.isRecord()) {
                    final String fieldProtoType = protoField.getMessageType().getName();
                    Type.Record r = ((Type.Record)fieldtype).withNameAndStorageName(ProtoUtils.toProtoBufCompliantName(fieldProtoType), fieldProtoType);
                    newFields.add(Type.Record.Field.of(r, field.getFieldNameOptional(), field.getFieldIndexOptional()));
                } else {
                    newFields.add(field);
                }
            }
            recordLayerType = Type.Record.fromFields(recordLayerType.isNullable(), newFields.build()).withNameAndStorageName(userName, storageName);
        }
        return RecordLayerTable.Builder
                .from(recordLayerType)
                .setPrimaryKey(recordType.getPrimaryKey())
                .addIndexes(recordType.getIndexes().stream().map(index -> RecordLayerIndex.from(userName, storageName, index)).collect(Collectors.toSet()));
    }

    @Nonnull
    @VisibleForTesting
    protected Function<Boolean, UserDefinedFunction> getSqlFunctionCompiler(@Nonnull final String name,
                                                                            @Nonnull final Supplier<RecordLayerSchemaTemplate> metadata,
                                                                            @Nonnull final String functionBody) {
        return isCaseSensitive -> RoutineParser.sqlFunctionParser(metadata.get()).parseFunction(functionBody, isCaseSensitive);
    }

    @Nonnull
    @VisibleForTesting
    protected Function<Boolean, LogicalOperator> getViewCompiler(@Nonnull final String viewName,
                                                                 @Nonnull final Supplier<RecordLayerSchemaTemplate> metadata,
                                                                 @Nonnull final String viewDefinition) {
        return isCaseSensitive -> RoutineParser.sqlFunctionParser(metadata.get()).parseView(viewName, viewDefinition, isCaseSensitive);
    }

    @Nonnull
    private RecordLayerInvokedRoutine.Builder generateInvokedRoutineBuilder(@Nonnull final Supplier<RecordLayerSchemaTemplate> metadata,
                                                                            @Nonnull final String name,
                                                                            @Nonnull final String body) {
        return RecordLayerInvokedRoutine.newBuilder()
                .setName(name)
                .setDescription(body)
                .setTemporary(false)
                .withUserDefinedRoutine(getSqlFunctionCompiler(name, metadata, body))
                .withSerializableFunction(new RawSqlFunction(name, body));
    }

    @Nonnull
    private RecordLayerInvokedRoutine.Builder generateInvokedRoutineBuilder(@Nonnull final String name,
                                                                            @Nonnull final String body,
                                                                            @Nonnull final UserDefinedMacroFunction userDefinedScalarFunction) {
        return RecordLayerInvokedRoutine.newBuilder()
                .setName(name)
                .setDescription(body)
                .withUserDefinedRoutine(ignored -> userDefinedScalarFunction)
                .withSerializableFunction(userDefinedScalarFunction);
    }

    @Nonnull
    @SuppressWarnings("PMD.UnusedFormalParameter") // metadata will be used for view compilation in the future
    private RecordLayerView.Builder generateViewBuilder(@Nonnull final Supplier<RecordLayerSchemaTemplate> metadata,
                                                        @Nonnull final String name,
                                                        @Nonnull final String definition) {
        return RecordLayerView.newBuilder()
                .setName(name)
                .setDescription(definition)
                .setViewCompiler(getViewCompiler(name, metadata, definition));
    }

    @Nonnull
    public RecordMetaData getRecordMetaData() {
        return recordMetaData;
    }
}

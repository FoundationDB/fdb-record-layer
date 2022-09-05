/*
 * TypingContext.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.TableInfo;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.ddl.SchemaTemplateDescriptor;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Holds information about Relational tables and Relational types that incrementally build during parsing.
 * It uses the {@link Type} system to represent these metadata objects and leverages it to generate the
 * internal representation for persistence.
 */
public final class TypingContext {

    @Nonnull
    private final TypeRepository.Builder typeRepositoryBuilder;

    @Nonnull
    private final Set<TypeDefinition> types;

    @Nonnull
    private final Multimap<String, Pair<RecordMetaDataProto.Index, List<String>>> indexes;

    private TypingContext() {
        typeRepositoryBuilder = TypeRepository.newBuilder();
        types = new LinkedHashSet<>();
        indexes = LinkedHashMultimap.create();
    }

    public void addIndex(@Nonnull final String name, @Nonnull final RecordMetaDataProto.Index index, @Nonnull final List<String> columnNames) {
        this.indexes.put(name, Pair.of(index, columnNames));
    }

    public void addType(@Nonnull final TypeDefinition typeDefinition) {
        this.types.add(typeDefinition);
    }

    @SuppressWarnings("PMD.LooseCoupling")
    public SchemaTemplate generateSchemaTemplate(@Nonnull final String name) {
        final TypeRepository repository = typeRepositoryBuilder.build();
        final var tableInfos = types.stream()
                .filter(type -> type.isTable)
                .map(typeDef -> {
                    List<RecordMetaDataProto.Index> tableIndexes = indexes.get(typeDef.name).stream().map(Pair::getLeft).collect(Collectors.toList());
                    KeyExpression primaryKey = createKeyExpression(typeDef);
                    return new TableInfo(typeDef.name, primaryKey,
                            tableIndexes, Objects.requireNonNull(repository.getMessageDescriptor(typeDef.name)).toProto());
                })
                .collect(Collectors.toList());
        Assert.thatUnchecked(!tableInfos.isEmpty(), "A schema template must declare at least one table", ErrorCode.INVALID_SCHEMA_TEMPLATE);
        final var typeInfos = types.stream().filter(type -> !type.isTable).map(t -> new TypeInfo(repository.getMessageDescriptor(t.name).toProto())).collect(Collectors.toSet());
        // add the rest of the types
        final var residualTypeInfos = repository.getMessageTypes().stream()
                .filter(type -> Stream.concat(tableInfos.stream().map(TableInfo::getTableName), typeInfos.stream().map(TypeInfo::getTypeName))
                        .noneMatch(included -> included.equals(type)))
                .map(t -> new TypeInfo(repository.getMessageDescriptor(t).toProto()));
        final var allTypeInfos = Streams.concat(typeInfos.stream(), residualTypeInfos).collect(Collectors.toSet());
        return new SchemaTemplateDescriptor(name, new LinkedHashSet<>(tableInfos), allTypeInfos, 1L);
    }

    private KeyExpression createKeyExpression(TypeDefinition typeDef) {
        if (typeDef.primaryKey.isEmpty()) {
            return Key.Expressions.recordType();
        } else {
            List<String> pkFields = typeDef.primaryKey.get();
            if (pkFields.isEmpty()) {
                return Key.Expressions.recordType();
            } else {
                Stream<KeyExpression> fieldExprs = pkFields.stream().map(Key.Expressions::field);
                return Key.Expressions.concat(Stream.concat(Stream.of(Key.Expressions.recordType()), fieldExprs).collect(Collectors.toList()));
            }
        }
    }

    public void addAllToTypeRepository() {
        verify();
        final var deps = generateTypeDependencyGraph();
        var typeDefinitions = TopologicalSort.anyTopologicalOrderPermutation(types, id -> deps.getOrDefault(id, ImmutableSet.of()));
        Assert.thatUnchecked(typeDefinitions.isPresent(), "Invalid cyclic dependency in the schema definition", ErrorCode.INVALID_SCHEMA_TEMPLATE);
        for (TypeDefinition t : typeDefinitions.get()) {
            List<Type.Record.Field> fields = IntStream.range(0, t.fields.size())
                    .mapToObj(i -> t.fields.get(i).toField(typeRepositoryBuilder, i + 1))
                    .collect(Collectors.toList());
            typeRepositoryBuilder.addTypeIfNeeded(ReferentialRecord.fromFieldsWithName(t.name, false, fields));
        }
    }

    @Nonnull
    public Set<String> getTableNames() {
        return types.stream().filter(t -> t.isTable).map(t -> t.name).collect(Collectors.toSet());
    }

    @Nonnull
    public Set<String> getIndexNames() {
        return indexes.keySet();
    }

    @Nonnull
    public TypeRepository.Builder getTypeRepositoryBuilder() {
        return typeRepositoryBuilder;
    }

    public static TypingContext create() {
        return new TypingContext();
    }

    @Nonnull
    private Map<TypeDefinition, Set<TypeDefinition>> generateTypeDependencyGraph() {
        ImmutableMap.Builder<TypeDefinition, Set<TypeDefinition>> result = ImmutableMap.builder();
        for (var type : types) {
            final var dependencyTypes = type.getDependencies()
                    .stream().map(dep -> types.stream()
                    .filter(t -> t.name.equals(dep))
                    .findFirst().orElseThrow())
                    .collect(Collectors.toSet());
            result.put(type, dependencyTypes);
        }
        return result.build();
    }

    private void verify() {
        // type dependencies are self-contained here.
        types.forEach(type -> type.getDependencies().forEach(dep -> Assert.thatUnchecked(types.stream().anyMatch(t -> dep.equals(t.name)), String.format("could not find type %s", dep))));
        // check that indexes have unique names.
        final var duplicateIndexNames = indexes.values().stream().map(pair -> pair.getLeft().getName())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(p -> p.getValue() > 1).map(Map.Entry::getKey).collect(Collectors.toList());
        Assert.thatUnchecked(duplicateIndexNames.isEmpty(), String.format("indices are duplicated %s", String.join(",", duplicateIndexNames)), ErrorCode.INDEX_ALREADY_EXISTS);
        // check that indexes reference existing tables.
        final var unknownTables = indexes.keySet().stream().filter(ref -> !(types.stream().filter(t -> t.isTable).map(t -> t.name).collect(Collectors.toSet()).contains(ref))).collect(Collectors.toSet());
        Assert.thatUnchecked(unknownTables.isEmpty(), String.format("unknown table(s) referenced in index definition(s) '%s'", String.join(",", unknownTables)), ErrorCode.UNDEFINED_TABLE);
        // indexes should reference non-existing columns
        indexes.forEach((table, pair) -> {
            var unknownFields = pair.getRight().stream().filter(indexedField -> types.stream().filter(t -> t.name.equals(table)).findFirst().get().fields.stream().map(f -> f.name).noneMatch(fName -> fName.equals(indexedField))).collect(Collectors.toSet());
            Assert.thatUnchecked(unknownFields.isEmpty(), String.format("index %s references non-existing field(s) (%s)", pair.getLeft().getName(), String.join(",", unknownFields)), ErrorCode.INVALID_COLUMN_REFERENCE);
        });
    }

    public Type getType(String structName) {
        return typeRepositoryBuilder.getTypeByName(structName).orElseThrow();
    }

    /**
     * Encapsulate preliminary information about a record or a type, this is usually used to hold parser results.
     */
    public static class TypeDefinition {
        @Nonnull
        final String name;
        @Nonnull
        final List<FieldDefinition> fields;

        /**
         * This flag is set when we want this type to be registered in the schema template Union message such that
         * it gets keyed and becomes visible for e.g. querying.
         */
        final boolean isTable;

        @Nonnull
        final Optional<List<String>> primaryKey;

        public TypeDefinition(@Nonnull final String name, @Nonnull final List<FieldDefinition> fields, boolean isRelationalTable, Optional<List<String>> primaryKey) {
            this.name = name;
            this.fields = fields;
            this.isTable = isRelationalTable;
            this.primaryKey = primaryKey;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof TypeDefinition)) {
                return false;
            }
            return name.equals(((TypeDefinition) obj).name);
        }

        @Nonnull
        public Set<String> getDependencies() {
            return fields.stream().filter(f -> !f.isPrimitive()).map(FieldDefinition::getTypeName).collect(Collectors.toSet());
        }

        public List<FieldDefinition> getFields() {
            return fields;
        }
    }

    /**
     * Encapsulate preliminary information about a record field, this is usually used to hold parser results.
     */
    public static class FieldDefinition {
        @Nonnull
        final String name;

        @Nonnull
        final Type.TypeCode pbType;

        @Nullable
        final String typeName;

        final boolean isRepeated;

        public FieldDefinition(@Nonnull final String name, @Nonnull final Type.TypeCode pbType, @Nullable final String typeName, boolean isRepeated) {
            this.name = name;
            this.pbType = pbType;
            this.typeName = typeName;
            this.isRepeated = isRepeated;
        }

        @Nonnull
        public Type.Record.Field toField(@Nonnull final TypeRepository.Builder typeRepository, int index) {
            Assert.thatUnchecked(index >= 0);
            if (pbType.isPrimitive()) {
                final Type type = Type.primitiveType(pbType, false);
                return Type.Record.Field.of(isRepeated ? new Type.Array(type) : type, Optional.of(name), Optional.of(index));
            } else {
                Assert.notNullUnchecked(typeName);
                Assert.thatUnchecked(typeRepository.getTypeByName(typeName).isPresent(), String.format("could not find type %s of record %s", typeName, name));
                final Type type = typeRepository.getTypeByName(typeName).get();
                return Type.Record.Field.of(isRepeated ? new Type.Array(type) : type, Optional.of(name), Optional.of(index));
            }
        }

        @Nullable
        public String getTypeName() {
            return typeName;
        }

        @Nonnull
        public String getFieldName() {
            return name;
        }

        private boolean isPrimitive() {
            return pbType.isPrimitive();
        }

    }

    @Nonnull
    public Map<String, Descriptors.FieldDescriptor> getFieldDescriptorMap() {
        addAllToTypeRepository();
        final var typeRepository = typeRepositoryBuilder.build();
        return getTableNames().stream().map(typeRepository::getMessageDescriptor).filter(Objects::nonNull).flatMap(r -> r.getFields().stream())
                .collect(Collectors.groupingBy(Descriptors.FieldDescriptor::getName,
                        Collectors.reducing(null,
                                (fieldDescriptor, fieldDescriptor2) -> {
                                    Verify.verify(fieldDescriptor != null || fieldDescriptor2 != null);
                                    if (fieldDescriptor == null) {
                                        return fieldDescriptor2;
                                    }
                                    if (fieldDescriptor2 == null) {
                                        return fieldDescriptor;
                                    }
                                    // TODO improve
                                    if (fieldDescriptor.getType().getJavaType() ==
                                            fieldDescriptor2.getType().getJavaType()) {
                                        return fieldDescriptor;
                                    }

                                    throw new IllegalArgumentException("cannot form union type of complex fields");
                                })));
    }
}

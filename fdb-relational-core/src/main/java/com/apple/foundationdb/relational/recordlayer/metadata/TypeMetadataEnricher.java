/*
 * TypeMetadataEnricher.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for enriching DataType structures with metadata from semantic analysis and protobuf descriptors.
 *
 * <p>This class handles the merging of type information from multiple sources:
 * <ul>
 *   <li>Field names from the planner's Type.Record (which handles aliases, star expansion, etc.)</li>
 *   <li>Type structure from semantic DataTypes (which preserves struct type names like "STRUCT_1")</li>
 *   <li>Additional enrichment from RecordMetaData descriptors for nested types</li>
 * </ul>
 *
 * <p>The planner's Type.Record loses struct type names during optimization (they become null),
 * but semantic analysis preserves them. This utility merges both sources to create complete
 * type metadata for result sets.
 */
@API(API.Status.EXPERIMENTAL)
public final class TypeMetadataEnricher {

    private TypeMetadataEnricher() {
        // Utility class - prevent instantiation
    }

    /**
     * Merge semantic type structure (preserving struct type names) with planner field names.
     *
     * <p>This method combines:
     * <ul>
     *   <li>Field names and field count from planner Type.Record
     *       (planner handles aliases, star expansion, and "_0" naming for unnamed expressions)</li>
     *   <li>Type structure (especially nested struct type names) from semantic DataTypes
     *       (semantic analysis preserves "STRUCT_1", "STRUCT_2" which planner loses)</li>
     *   <li>Additionally enrich nested structs with RecordMetaData descriptor names</li>
     * </ul>
     *
     * @param plannerType The Type.Record from the physical plan (has correct field names)
     * @param semanticTypes The semantic DataTypes captured before planning (have struct type names)
     * @param recordMetaData Schema metadata for enriching nested types
     * @return Merged DataType.StructType with planner names and semantic type structure
     * @throws RelationalException if type structures don't match
     */
    @Nonnull
    public static DataType.StructType mergeSemanticTypesWithPlannerNames(
            @Nonnull final Type plannerType,
            @Nonnull final List<DataType> semanticTypes,
            @Nonnull final RecordMetaData recordMetaData) throws RelationalException {

        Assert.that(plannerType instanceof Type.Record, ErrorCode.INTERNAL_ERROR,
                "Expected Type.Record but got %s", plannerType.getTypeCode());

        final Type.Record recordType = (Type.Record) plannerType;
        final List<Type.Record.Field> plannerFields = recordType.getFields();

        // Planner and semantic should have same field count
        Assert.that(plannerFields.size() == semanticTypes.size(), ErrorCode.INTERNAL_ERROR,
                "Field count mismatch: planner has %d fields, semantic has %d",
                plannerFields.size(), semanticTypes.size());

        // Build descriptor cache for enriching nested structs
        final Map<String, Descriptor> descriptorCache = new HashMap<>();
        for (var recordTypeEntry : recordMetaData.getRecordTypes().values()) {
            cacheDescriptorAndNested(recordTypeEntry.getDescriptor(), descriptorCache);
        }
        final var fileDescriptor = recordMetaData.getRecordTypes().values().iterator().next()
                .getDescriptor().getFile();
        for (var messageType : fileDescriptor.getMessageTypes()) {
            cacheDescriptorAndNested(messageType, descriptorCache);
        }

        // Merge: field names from planner, types from semantic (enriched)
        final ImmutableList.Builder<DataType.StructType.Field> mergedFields = ImmutableList.builder();
        for (int i = 0; i < plannerFields.size(); i++) {
            final String fieldName = plannerFields.get(i).getFieldName();
            final DataType enrichedType = enrichDataType(semanticTypes.get(i), descriptorCache);
            mergedFields.add(DataType.StructType.Field.from(fieldName, enrichedType, i));
        }

        return DataType.StructType.from("QUERY_RESULT", mergedFields.build(), true);
    }

    /**
     * Cache a descriptor and all its nested types, keyed by their structural signature.
     *
     * @param descriptor The protobuf descriptor to cache
     * @param cache The cache map to populate
     */
    private static void cacheDescriptorAndNested(@Nonnull final Descriptor descriptor,
                                                  @Nonnull final Map<String, Descriptor> cache) {
        // Create a structural signature for this descriptor (field names and count)
        final String signature = createStructuralSignature(descriptor);
        cache.put(signature, descriptor);

        // Process nested types
        for (var nestedType : descriptor.getNestedTypes()) {
            cacheDescriptorAndNested(nestedType, cache);
        }
    }

    /**
     * Create a structural signature for a descriptor based on field names only.
     * Field indices can vary between DataType and protobuf representations.
     *
     * @param descriptor The protobuf descriptor
     * @return A signature string representing the structure
     */
    @Nonnull
    private static String createStructuralSignature(@Nonnull final Descriptor descriptor) {
        return descriptor.getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(java.util.stream.Collectors.joining(","));
    }

    /**
     * Create a structural signature for a DataType.StructType based on field names only.
     *
     * @param structType The struct type
     * @return A signature string representing the structure
     */
    @Nonnull
    private static String createStructuralSignature(@Nonnull final DataType.StructType structType) {
        return structType.getFields().stream()
                .map(DataType.StructType.Field::getName)
                .collect(java.util.stream.Collectors.joining(","));
    }

    /**
     * Recursively enrich a struct type with proper names from the descriptor cache.
     *
     * @param structType The struct type to enrich
     * @param descriptorCache Cache of descriptors keyed by structural signature
     * @return Enriched struct type with proper names from descriptors
     */
    @Nonnull
    private static DataType.StructType enrichStructType(@Nonnull final DataType.StructType structType,
                                                         @Nonnull final Map<String, Descriptor> descriptorCache) {
        // Enrich each field recursively
        final List<DataType.StructType.Field> enrichedFields = structType.getFields().stream()
                .map(field -> enrichField(field, descriptorCache))
                .collect(java.util.stream.Collectors.toList());

        // Try to find a matching descriptor for this struct type
        final String signature = createStructuralSignature(structType);
        final Descriptor matchedDescriptor = descriptorCache.get(signature);

        // Use the descriptor's name if found, otherwise keep the existing name
        final String enrichedName = matchedDescriptor != null ? matchedDescriptor.getName() : structType.getName();

        return DataType.StructType.from(enrichedName, enrichedFields, structType.isNullable());
    }

    /**
     * Enrich a field, recursively enriching any nested struct types.
     *
     * @param field The field to enrich
     * @param descriptorCache Cache of descriptors keyed by structural signature
     * @return Enriched field with proper type metadata
     */
    @Nonnull
    private static DataType.StructType.Field enrichField(@Nonnull final DataType.StructType.Field field,
                                                          @Nonnull final Map<String, Descriptor> descriptorCache) {
        final DataType enrichedType = enrichDataType(field.getType(), descriptorCache);
        return DataType.StructType.Field.from(field.getName(), enrichedType, field.getIndex());
    }

    /**
     * Enrich a DataType, handling structs, arrays, and primitives.
     *
     * <p>For struct types, looks up matching descriptors and enriches the struct name.
     * For array types, recursively enriches the element type.
     * For primitive types, returns as-is.
     *
     * @param dataType The data type to enrich
     * @param descriptorCache Cache of descriptors keyed by structural signature
     * @return Enriched data type with proper metadata
     */
    @Nonnull
    private static DataType enrichDataType(@Nonnull final DataType dataType,
                                            @Nonnull final Map<String, Descriptor> descriptorCache) {
        if (dataType instanceof DataType.StructType) {
            return enrichStructType((DataType.StructType) dataType, descriptorCache);
        } else if (dataType instanceof DataType.ArrayType) {
            final DataType.ArrayType arrayType = (DataType.ArrayType) dataType;
            final DataType enrichedElementType = enrichDataType(arrayType.getElementType(), descriptorCache);
            return DataType.ArrayType.from(enrichedElementType, arrayType.isNullable());
        } else {
            // Primitive types don't need enrichment
            return dataType;
        }
    }
}

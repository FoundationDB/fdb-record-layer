/*
 * RelationalParserContext.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Scopes;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

public class RelationalParserContext extends ParserContext {

    @Nonnull
    private final ImmutableSet.Builder<CorrelationIdentifier> aliases;

    // this field holds all the relations (record types) that we want to filter-scan from. Currently, it is needed
    // upfront by the planner, and it should become unnecessary later on.
    @Nonnull
    private final Set<String> filteredRecords;

    @Nonnull
    private final Set<String> scannableRecordTypeNames;
    @Nonnull
    private final Map<String, Descriptors.FieldDescriptor> scannabledRecordTypes;

    @Nonnull
    private final Set<String> indexNames;

    private RelationalParserContext(@Nonnull final Scopes scopes,
                                  @Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final ImmutableSet.Builder<CorrelationIdentifier> aliases,
                                  @Nonnull final Set<String> filteredRecords,
                                  @Nonnull final Set<String> scannableRecordTypeNames,
                                  @Nonnull final Map<String, Descriptors.FieldDescriptor> scannabledRecordTypes,
                                  @Nonnull final Set<String> indexNames) {
        super(scopes, typeRepositoryBuilder);
        this.aliases = aliases;
        this.filteredRecords = filteredRecords;
        this.scannableRecordTypeNames = scannableRecordTypeNames;
        this.scannabledRecordTypes = scannabledRecordTypes;
        this.indexNames = indexNames;
    }

    public RelationalParserContext(@Nonnull final Scopes scopes,
                                 @Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                 @Nonnull final Set<String> scannableRecordTypeNames,
                                 @Nonnull final Map<String, Descriptors.FieldDescriptor> scannabledRecordTypes,
                                 @Nonnull final Set<String> indexNames) {
        this(scopes, typeRepositoryBuilder, ImmutableSet.builder(), new HashSet<>(), scannableRecordTypeNames, scannabledRecordTypes, indexNames);
    }

    public void addFilteredRecord(@Nonnull final String recordType) {
        filteredRecords.add(recordType);
    }

    @Nonnull
    public Set<String> getFilteredRecords() {
        return filteredRecords;
    }

    @Nonnull
    public RelationalParserContext withTypeRepositoryBuilder(@Nonnull TypeRepository.Builder builder) {
        return new RelationalParserContext(getScopes(), builder, aliases, filteredRecords, scannableRecordTypeNames, scannabledRecordTypes, indexNames);
    }

    @Nonnull
    public RelationalParserContext withScannableRecordTypes(@Nonnull final Set<String> scannableRecordTypeNames, @Nonnull final Map<String, Descriptors.FieldDescriptor> scannabledRecordTypes) {
        return new RelationalParserContext(getScopes(), getTypeRepositoryBuilder(), aliases, filteredRecords, scannableRecordTypeNames, scannabledRecordTypes, indexNames);
    }

    @Nonnull
    public RelationalParserContext withIndexNames(@Nonnull final Set<String> indexNames) {
        return new RelationalParserContext(getScopes(), getTypeRepositoryBuilder(), aliases, filteredRecords, scannableRecordTypeNames, scannabledRecordTypes, indexNames);
    }

    @Nonnull
    public Map<String, Descriptors.FieldDescriptor> getScannabledRecordTypes() {
        return scannabledRecordTypes;
    }

    @Nonnull
    public Set<String> getScannableRecordTypeNames() {
        return scannableRecordTypeNames;
    }

    @Nonnull
    public Set<String> getIndexNames() {
        return indexNames;
    }
}

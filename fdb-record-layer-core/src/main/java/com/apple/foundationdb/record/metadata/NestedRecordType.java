/*
 * NestedRecordType.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.Collections;

/**
 * Extension of record type representing a nested message within a larger record type. This is used to record
 * the descriptor of a nested message (within, for example, a synthetic {@link UnnestedRecordType}). Records
 * of this type cannot be directly saved to a record store.
 */
@API(API.Status.EXPERIMENTAL)
public class NestedRecordType extends RecordType {
    @API(API.Status.INTERNAL)
    public NestedRecordType(@Nonnull final RecordMetaData metaData,
                            @Nonnull final Descriptors.Descriptor descriptor,
                            @Nonnull final RecordType parentType,
                            @Nonnull final KeyExpression primaryKey) {
        super(metaData, descriptor, primaryKey, Collections.emptyList(), Collections.emptyList(), parentType.getSinceVersion(), parentType.getRecordTypeKey());
    }
}

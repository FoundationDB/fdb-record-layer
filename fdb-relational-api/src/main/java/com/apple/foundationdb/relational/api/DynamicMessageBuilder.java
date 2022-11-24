/*
 * DynamicMessageBuilder.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Interface to hide away the annoying details of building a Dynamic Message from a descriptor.
 */
@NotThreadSafe
public interface DynamicMessageBuilder {

    Set<String> getFieldNames();

    /**
     * Get the type for the specified field, or throw an error if the field doesn't exist.
     *
     * @param fieldName the name of the field to get the type for
     * @return the string name of the type that the field is.
     * @throws SQLException with {@link ErrorCode#INVALID_PARAMETER} if the field does not exist.
     */
    String getFieldType(String fieldName) throws SQLException;

    boolean isPrimitive(int fieldNumber) throws SQLException;

    DynamicMessageBuilder setField(String fieldName, Object value) throws SQLException;

    DynamicMessageBuilder setField(int fieldNumber, Object value) throws SQLException;

    DynamicMessageBuilder addRepeatedField(String fieldName, Object value) throws SQLException;

    DynamicMessageBuilder addRepeatedField(int fieldNumber, Object value) throws SQLException;

    DynamicMessageBuilder addRepeatedFields(String fieldName, Iterable<? extends Object> value, boolean isNullableArray) throws SQLException;

    DynamicMessageBuilder addRepeatedFields(String fieldName, Iterable<? extends Object> value) throws SQLException;

    DynamicMessageBuilder addRepeatedFields(int fieldNumber, Iterable<? extends Object> value) throws SQLException;

    Message build();

    <T extends Message> Message convertMessage(T m) throws SQLException;

    DynamicMessageBuilder getNestedMessageBuilder(String fieldName) throws SQLException;

    DynamicMessageBuilder getNestedMessageBuilder(int fieldNumber) throws SQLException;

    Descriptors.Descriptor getDescriptor();

    @Nonnull
    DynamicMessageBuilder newBuilder();
}

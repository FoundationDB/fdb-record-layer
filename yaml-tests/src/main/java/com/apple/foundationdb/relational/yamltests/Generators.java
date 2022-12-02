/*
 * Generators.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.apple.foundationdb.relational.recordlayer.util.Assert.fail;
import static com.apple.foundationdb.relational.yamltests.Matchers.*;

public class Generators {

    public static Collection<Message> yamlToDynamicMessage(@Nullable Object value, @Nonnull DynamicMessageBuilder dmBuilder) throws RelationalException {
        if (isNull(value)) {
            return Collections.emptySet();
        } else if (isArray(value)) {
            List<Message> messages = new ArrayList<>();
            for (Object v : arrayList(value)) {
                messages.addAll(yamlToDynamicMessage(v, dmBuilder.newBuilder()));
            }
            return messages;
        } else if (isMap(value)) {
            return Collections.singleton(parseObject(map(value), dmBuilder));
        }
        fail("Cannot parse yaml <" + value + ">, It must be an Object, Array, or NULL", ErrorCode.SYNTAX_ERROR);
        return null;
    }

    private static Message parseObject(@Nonnull final Map<?, ?> data, @Nonnull final DynamicMessageBuilder typeBuilder) throws RelationalException {
        final BiConsumer<Object, Object> fieldSetter = (fieldAccessor, fieldValue) -> {
            try {
                if (fieldAccessor instanceof Integer) {
                    typeBuilder.setField((Integer) fieldAccessor, fieldValue);
                } else {
                    typeBuilder.setField(string(fieldAccessor), fieldValue);
                }
            } catch (SQLException e) {
                throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
            }
        };
        int counter = 1;
        for (Map.Entry<?, ?> entry : data.entrySet()) {
            if (isNull(entry.getValue())) { // unnamed field.
                setField(counter, entry.getKey(), fieldSetter, typeBuilder, true);
            } else { // named field.
                String key = string(entry.getKey(), "field key");
                setField(key, entry.getValue(), fieldSetter, typeBuilder, true);
            }
            ++counter;
        }
        return typeBuilder.build();
    }

    private static void setField(Object key,
                                 Object value,
                                 BiConsumer<Object, Object> typeConsumer,
                                 DynamicMessageBuilder dataBuilder,
                                 boolean allowArrays) throws RelationalException {
        try {
            if (value instanceof YamlRunner.NullPlaceholder) {
                // do not set the value!
                return;
            } else if (isArray(value)) {
                if (!allowArrays) {
                    throw new RelationalException("Cannot nest arrays within arrays!", ErrorCode.INVALID_PARAMETER);
                }
                DynamicMessageBuilder wrappedArrayBuilder = (key instanceof Integer) ? dataBuilder.getNestedMessageBuilder((int) key) : dataBuilder.getNestedMessageBuilder(string(key, "field descriptor"));
                final List<?> array = arrayList(value);
                for (Object arrayValue : array) {
                    setField("values", arrayValue, (fieldIndicator, fieldValue) -> {
                        try {
                            if (fieldIndicator instanceof Integer) {
                                wrappedArrayBuilder.addRepeatedField((int) fieldIndicator, fieldValue);
                            } else {
                                wrappedArrayBuilder.addRepeatedField(string(fieldIndicator, "field descriptor"), fieldValue);
                            }
                        } catch (SQLException e) {
                            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
                        }
                    }, wrappedArrayBuilder, false);
                    typeConsumer.accept(key, wrappedArrayBuilder.build());
                }
                return;
            } else if (isMap(value)) {
                DynamicMessageBuilder nestedBuilder = (key instanceof Integer) ? dataBuilder.getNestedMessageBuilder((int) key) : dataBuilder.getNestedMessageBuilder(string(key, "field descriptor"));
                Message underlying = parseObject(map(value), nestedBuilder);
                typeConsumer.accept(key, underlying);
                return;
            } else if (isString(value)) {
                typeConsumer.accept(key, string(value));
                return;
            } else if (isBoolean(value)) {
                typeConsumer.accept(key, bool(value));
                return;
            } else if (isLong(value)) {
                typeConsumer.accept(key, longValue(value));
                return;
            } else if (isInt(value)) {
                typeConsumer.accept(key, intValue(value));
                return;
            } else if (isDouble(value)) {
                typeConsumer.accept(key, doubleValue(value));
                return;
            }
            fail(String.format("unexpected type %s", value));
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }
}

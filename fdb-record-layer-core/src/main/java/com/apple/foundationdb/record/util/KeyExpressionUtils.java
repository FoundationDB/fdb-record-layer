/*
 * KeyExpressionUtils.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;


import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Set of utility functions that support transforming a {@link com.apple.foundationdb.record.query.plan.cascades.values.Value}
 * to a {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} and vice versa.
 */
public class KeyExpressionUtils {

    public static boolean convertibleToKeyExpression(@Nonnull final Value value) {
        return value instanceof FieldValue;
    }

    @Nonnull
    public static KeyExpression toKeyExpression(@Nonnull final Value value) {
        if (value instanceof FieldValue) {
            final var fieldValue = (FieldValue)value;
            final var expressions = fieldValue.getFieldPath().getFieldAccessors().stream().map(KeyExpressionUtils::toKeyExpression).collect(Collectors.toList());
            Verify.verify(!expressions.isEmpty());
            KeyExpression result = expressions.get(expressions.size() - 1);
            if (expressions.size() == 1) {
                return result;
            }
            for (int i = expressions.size() - 1; i >= 0; --i) {
                result = expressions.get(i).nest(result);
            }
            return result;
        }
        throw new RecordCoreException(String.format("converting %s to %s is not supported", value.getClass(), KeyExpression.class));
    }

    @Nonnull
    private static FieldKeyExpression toKeyExpression(@Nonnull final FieldValue.ResolvedAccessor accessor) {
        final var fanType = accessor.getType().getTypeCode() == Type.TypeCode.ARRAY ?
                            KeyExpression.FanType.FanOut :
                            KeyExpression.FanType.None;
        return field(Objects.requireNonNull(accessor.getName()), fanType);
    }

}

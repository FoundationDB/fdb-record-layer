/*
 * LiteralsUtils.java
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class LiteralsUtils {

    @Nonnull
    public static Value processLiteral(@Nonnull final Value value,
                                       @Nullable final Object literal,
                                       @Nonnull final PlanGenerationContext context) {
        if (!context.shouldProcessLiteral()) {
            return value;
        } else {
            final var literalIndex = context.addStrippedLiteral(literal);
            final var result = ConstantObjectValue.of(Quantifier.constant(), literalIndex, value.getResultType());
            context.addLiteralReference(result);
            return result;
        }
    }

    @Nonnull
    public static Value processArrayLiteral(@Nonnull final List<Value> values,
                                            int index,
                                            @Nonnull final PlanGenerationContext context) {
        Assert.thatUnchecked(!values.isEmpty());
        // all values must have the same type.
        final var valueTypes = values.stream().map(Value::getResultType).filter(type -> type != Type.nullType()).distinct().collect(Collectors.toList());
        Assert.thatUnchecked(valueTypes.size() == 1, "could not determine type of array literal", ErrorCode.DATATYPE_MISMATCH);
        final var result = ConstantObjectValue.of(Quantifier.constant(), index, new Type.Array(valueTypes.get(0)));
        if (context.shouldProcessLiteral()) {
            context.addLiteralReference(result);
        }
        return result;
    }

    @Nonnull
    public static QueryPredicate toQueryPredicate(@Nonnull final BooleanValue value,
                                                  @Nonnull CorrelationIdentifier innermostAlias,
                                                  @Nonnull final PlanGenerationContext context) {
        if (context.hasDdlAncestor()) {
            final var result = value.toQueryPredicate(ParserUtils.EMPTY_TYPE_REPOSITORY, innermostAlias);
            Assert.thatUnchecked(result.isPresent());
            return result.get();
        } else {
            final var result = value.toQueryPredicate(null, innermostAlias);
            Assert.thatUnchecked(result.isPresent());
            return result.get();
        }
    }
}

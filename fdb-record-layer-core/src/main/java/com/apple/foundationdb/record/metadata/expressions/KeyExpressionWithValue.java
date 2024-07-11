/*
 * KeyExpressionWithValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.KeyExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A key expression that can be represented as a single {@link Value} because it meets both of the following criteria:
 * <ul>
 *     <li>It's a single value rather than a complex tuple.</li>
 *     <li>It does not produce multiple values because of fan-out.</li>
 * </ul>
 * This is completely unrelated to the (disturbingly) similarly named {@link KeyWithValueExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public interface KeyExpressionWithValue extends KeyExpression {
    @Nonnull
    default Value toValue(@Nonnull Quantifier baseQuantifier, @Nonnull List<String> fieldNamePrefix) {
        return toValue(baseQuantifier.getAlias(), baseQuantifier.getFlowedObjectType(), fieldNamePrefix);
    }

    @Nonnull
    Value toValue(@Nonnull CorrelationIdentifier baseAlias, @Nonnull Type baseType, @Nonnull List<String> fieldNamePrefix);

    @Nonnull
    @Override
    default <S extends KeyExpressionVisitor.State, R> R expand(@Nonnull KeyExpressionVisitor<S, R> visitor) {
        return visitor.visitExpression(this);
    }
}

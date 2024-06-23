/*
 * OrderFunctionKeyExpressionFactoryRE.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Factory for {@link OrderFunctionKeyExpression} for each supported direction.
 * Note that {@code order_asc_nulls_first} is included for completeness, since that is the default ordering and
 * no function is needed.
 */
@AutoService(FunctionKeyExpression.Factory.class)
@API(API.Status.EXPERIMENTAL)
public class OrderFunctionKeyExpressionFactory implements FunctionKeyExpression.Factory {
    public static final String FUNCTION_NAME_PREFIX = "order_";

    @Nonnull
    @Override
    public List<FunctionKeyExpression.Builder> getBuilders() {
        return Arrays.stream(TupleOrdering.Direction.values())
                .map(direction -> new FunctionKeyExpression.BiFunctionBuilder(FUNCTION_NAME_PREFIX + direction.name().toLowerCase(Locale.ROOT),
                        (name, arguments) -> new OrderFunctionKeyExpression(direction, name, arguments)))
                .collect(Collectors.toList());
    }
}

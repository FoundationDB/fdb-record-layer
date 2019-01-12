/*
 * CollateFunctionKeyExpressionFactoryJRE.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.API;
import com.google.auto.service.AutoService;
import com.apple.foundationdb.record.provider.common.text.TextCollatorRegistryJRE;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Implemention of {@link CollateFunctionKeyExpression} using {@link TextCollatorRegistryJRE}.
 *
 */
@AutoService(FunctionKeyExpression.Factory.class)
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:abbreviationaswordinname") // Allow JRE here.
public class CollateFunctionKeyExpressionFactoryJRE implements FunctionKeyExpression.Factory {
    public static final String FUNCTION_NAME = "collate_jre";

    @Nonnull
    @Override
    public List<FunctionKeyExpression.Builder> getBuilders() {
        return Collections.singletonList(
                new FunctionKeyExpression.BiFunctionBuilder(FUNCTION_NAME, CollateFunctionKeyExpressionJRE::new));
    }

    protected static class CollateFunctionKeyExpressionJRE extends CollateFunctionKeyExpression {
        protected CollateFunctionKeyExpressionJRE(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(TextCollatorRegistryJRE.instance(), name, arguments);
        }
    }
}

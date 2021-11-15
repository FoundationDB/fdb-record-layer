/*
 * LuceneFunctionKeyExpressionFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

/**
 * Implemention of Lucene index key functions.
 * @see LuceneFunctionNames
 */
@AutoService(FunctionKeyExpression.Factory.class)
@API(API.Status.EXPERIMENTAL)
public class LuceneFunctionKeyExpressionFactory implements FunctionKeyExpression.Factory {
    @Nonnull
    @Override
    public List<FunctionKeyExpression.Builder> getBuilders() {
        return Arrays.asList(
            new FunctionKeyExpression.BiFunctionBuilder(LuceneFunctionNames.LUCENE_FIELD_NAME, LuceneFunctionKeyExpression.LuceneFieldName::new),
            new FunctionKeyExpression.BiFunctionBuilder(LuceneFunctionNames.LUCENE_STORED, LuceneFunctionKeyExpression.LuceneStored::new),
            new FunctionKeyExpression.BiFunctionBuilder(LuceneFunctionNames.LUCENE_TEXT, LuceneFunctionKeyExpression.LuceneText::new)
        );
    }
}

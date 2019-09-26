/*
 * OneOfThemText.java
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

package com.apple.foundationdb.record.query.expressions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// Package private class used to create OneOfThemWithComparison components on text queries
class OneOfThemText extends Text {
    @Nonnull
    private final String fieldName;
    private final Field.OneOfThemEmptyMode emptyMode;

    OneOfThemText(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode) {
        this(fieldName, emptyMode, null);
    }

    OneOfThemText(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode, @Nullable String tokenizerName) {
        this(fieldName, emptyMode, tokenizerName, null);
    }

    OneOfThemText(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode, @Nullable String tokenizerName, @Nullable String defaultTokenizerName) {
        super(tokenizerName, defaultTokenizerName);
        this.fieldName = fieldName;
        this.emptyMode = emptyMode;
    }

    @Nonnull
    @Override
    ComponentWithComparison getComponent(@Nonnull Comparisons.Comparison comparison) {
        return new OneOfThemWithComparison(fieldName, emptyMode, comparison);
    }
}

/*
 * LuceneThenKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.List;

public class LuceneThenKeyExpression extends ThenKeyExpression implements LuceneKeyExpression {

    private LuceneKeyExpression primaryKey;
    boolean validated = false;

    public LuceneThenKeyExpression(@Nonnull final LuceneKeyExpression primaryKey, @Nonnull final List<KeyExpression> children) {
        super(children);
        if (!validateLucene()) throw new IllegalArgumentException("failed to validate lucene compatibility on construction");
        this.primaryKey = primaryKey;

    }

    @Override
    public boolean validateLucene() {
        if (validated) return validated;

        for (KeyExpression child : getChildren()) {
            validated = validated &&
                        child instanceof LuceneKeyExpression &&
                        ((LuceneKeyExpression)child).validateLucene();
        }

        return validated;
    }
}

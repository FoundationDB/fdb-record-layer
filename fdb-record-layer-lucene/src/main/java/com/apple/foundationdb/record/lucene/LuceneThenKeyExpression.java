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
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class LuceneThenKeyExpression extends ThenKeyExpression implements LuceneKeyExpression {

    // Only one key allowed per repeated field
    private LuceneFieldKeyExpression primaryKey;
    boolean validated = false;
    boolean isPrefixed = false;
    String prefix;


    public LuceneThenKeyExpression(@Nonnull final LuceneFieldKeyExpression primaryKey, @Nonnull final List<KeyExpression> children) {
        super(children);
        if (!validate()) throw new IllegalArgumentException("failed to validate lucene compatibility on construction");
        this.primaryKey = primaryKey;

    }

    public boolean validate() {
        if (validated) return validated;
        validated = true;
        for (KeyExpression child : getChildren()) {
            validated = validated && child instanceof LuceneKeyExpression;
        }
        return validated;
    }

    public void prefix(String prefix) {
        if (!(isPrefixed)){
            for (LuceneFieldKeyExpression child : getLuceneChildren()) {
                child.prefix(prefix);
            }
        }
    }

    @Override
    public List<KeyExpression> normalizeKeyForPositions(){
        return Lists.newArrayList(this);
    }

    public int getPrimaryKeyPosition() {
        return super.normalizeKeyForPositions().indexOf(primaryKey);
    }

    public List<LuceneFieldKeyExpression> getLuceneChildren(){
        List<LuceneFieldKeyExpression> children = getChildren().stream().map((e) -> (LuceneFieldKeyExpression)e )
                .collect(Collectors.toList());
        return children;
    }
}

/*
 * TypedFieldKeyExpression.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;

import javax.annotation.Nonnull;

public class LuceneFieldKeyExpression extends FieldKeyExpression implements LuceneKeyExpression {

    private FieldType type;
    private boolean sorted;
    private boolean stored;
    private boolean isPrefixed = false;
    private String prefix;

    public LuceneFieldKeyExpression(@Nonnull final String fieldName, @Nonnull final FanType fanType,
                                    @Nonnull final Key.Evaluated.NullStandin nullStandin, @Nonnull final FieldType type,
                                    @Nonnull final boolean sorted, @Nonnull final boolean stored) {
        super(fieldName, fanType, nullStandin);
        this.type = type;
        this.sorted = sorted;
        this.stored = stored;

    }

    public LuceneFieldKeyExpression(@Nonnull final String field, @Nonnull final FieldType type,
                                    @Nonnull final boolean sorted, @Nonnull final boolean stored) throws DeserializationException {
        this(field,FanType.None, Key.Evaluated.NullStandin.NULL,type,sorted,stored);
    }

    public LuceneFieldKeyExpression(@Nonnull final FieldKeyExpression original, @Nonnull final FieldType type,
                                    @Nonnull final boolean sorted, @Nonnull final boolean stored) throws DeserializationException {
        this(original.getFieldName(), original.getFanType(),original.getNullStandin(),type,sorted,stored);
    }

    public FieldType getType() {
        return type;
    }

    public boolean isStored() {
        return stored;
    }

    @Override
    public void prefix(String prefix) {
        if (!(isPrefixed)) {
            isPrefixed = true;
            this.prefix = prefix;
        }
    }

    public String getPrefixedFieldName() {
        if (isPrefixed) {
            return prefix.concat(getFieldName());
        }
        return getFieldName();
    }

    public String getPrefixedFieldName(String prefix) {
        String finalName = prefix;
        if (isPrefixed) {
            finalName = this.prefix.concat(finalName);
        }
        return finalName.concat(getFieldName());
    }
}

/*
 * LuceneStructField.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;

import javax.annotation.Nonnull;
import java.util.List;

public class LuceneNestingExpression extends NestingKeyExpression implements LuceneKeyExpression {

    private boolean validated = false;

    public LuceneNestingExpression(@Nonnull final LuceneFieldKeyExpression parent, @Nonnull final LuceneKeyExpression child) {
        super(parent, child);
    }

    public LuceneNestingExpression(@Nonnull final RecordMetaDataProto.Nesting nesting) throws DeserializationException {
        super(nesting);
        if (!(validateLucene())) throw new IllegalArgumentException("failed to validate lucene compatibility on construction");
    }

    public boolean validateLucene() {
        if (validated) return validated;

        if (!(getParent() instanceof LuceneFieldKeyExpression)) {
            validated = false;
            return false;
        } if (!(getChild() instanceof LuceneKeyExpression)) {
            validated = false;
            return false;
        }
        return ((LuceneKeyExpression) getChild()).validateLucene();
    }
}

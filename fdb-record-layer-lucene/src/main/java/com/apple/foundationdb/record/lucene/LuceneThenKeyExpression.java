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

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionVisitor;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class LuceneThenKeyExpression extends ThenKeyExpression implements LuceneKeyExpression {

    // Only one key allowed per repeated field
    private KeyExpression primaryKey;
    boolean validated = false;


    public LuceneThenKeyExpression(@Nullable final KeyExpression primaryKey, @Nonnull final List<KeyExpression> children) {
        super(children);
        if (!validate()) {
            throw new IllegalArgumentException("failed to validate lucene compatibility on construction");
        }
        this.primaryKey = primaryKey;

    }

    // Todo: figure out if we should allow stored key expressions inside a thenKeyExpression,
    // for now not but tbd if we should check the farOut and decide based on that.
    public boolean validate() {
        if (validated) {
            return validated;
        }
        validated = true;
        for (KeyExpression child : getChildren()) {
            validated = validated && (primaryKey != null) ? child instanceof LuceneFieldKeyExpression : (LuceneKeyExpression.validateLucene(child));
        }
        return validated;
    }

    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    public int getPrimaryKeyPosition() {
        return super.normalizeKeyForPositions().indexOf(primaryKey);
    }

    public List<LuceneFieldKeyExpression> getLuceneChildren() {
        List<LuceneFieldKeyExpression> children = new LinkedList<>();
        KeyExpressionVisitor visitor = new KeyExpressionVisitor() {

            @Override
            public KeyExpression visitField(final FieldKeyExpression fke) {
                if (fke instanceof LuceneFieldKeyExpression) {
                    children.add((LuceneFieldKeyExpression)fke);
                }
                return fke;
            }

            @Override
            public KeyExpression visitThen(final ThenKeyExpression thenKey) {
                for (KeyExpression child : thenKey.getChildren()) {
                    visit(child);
                }
                return thenKey;
            }

            @Override
            public KeyExpression visitNestingKey(final NestingKeyExpression nke) {
                visitField(nke.getParent());
                visit(nke.getChild());
                return nke;
            }

            @Override
            public KeyExpression visitGroupingKey(final GroupingKeyExpression gke) {
                visit(gke.getChild());
                return gke;
            }

            @Override
            public boolean applies(final String indexType) {
                return IndexTypes.LUCENE.equals(indexType);
            }
        };

        visitor.visit(this);

        return children;
    }

    public boolean fan() {
        return (primaryKey != null);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final LuceneThenKeyExpression that = (LuceneThenKeyExpression)o;
        return primaryKey.equals(that.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), primaryKey);
    }
}

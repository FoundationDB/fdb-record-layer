/*
 * LuceneTypeConverter.java
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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts from normal KeyExpression types to Lucene KE types.
 */
public class LuceneTypeConverter implements KeyExpressionVisitor {

    boolean fanNext = false;

    @Override
    public FieldKeyExpression visitField(final FieldKeyExpression fke) {
        fanNext = false;
        if (fke instanceof LuceneFieldKeyExpression) return fke;
        return new LuceneFieldKeyExpression(fke, LuceneKeyExpression.FieldType.STRING, true, true);
    }

    @Override
    public KeyExpression visitThen(final ThenKeyExpression thenKey) {
        boolean hasPrimaryExpression = fanNext;
        List<KeyExpression> children = thenKey.getChildren().stream()
                .map(this::visit).collect(Collectors.toList());
        if (!hasPrimaryExpression) {
            return new LuceneThenKeyExpression(null, children);
        }
        fanNext = false;
        return new LuceneThenKeyExpression(children.get(0), children);
    }

    @Override
    public KeyExpression visitNestingKey(final NestingKeyExpression nke) {
        if (nke.getParent().getFanType() == KeyExpression.FanType.FanOut) {
            fanNext = true;
        }
        KeyExpression child = visit(nke.getChild());
        return new NestingKeyExpression(visitField(nke.getParent()),child);
    }

    @Override
    public KeyExpression visitGroupingKey(final GroupingKeyExpression gke) {
        fanNext = false;
        return new GroupingKeyExpression(visit(gke.getChild()), gke.getGroupedCount());
    }

    @Override
    public boolean applies(final String indexType) {
        return IndexTypes.LUCENE.equals(indexType);
    }

}

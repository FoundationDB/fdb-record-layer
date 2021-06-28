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

import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts from normal KeyExpression types to Lucene KE types.
 */
public class LuceneTypeConverter {

    public static KeyExpression convert(KeyExpression original) {
        if (original instanceof FieldKeyExpression) {
            //TODO(bfines) get that information from somewhere in the index definition
            return new LuceneFieldKeyExpression((FieldKeyExpression)original, LuceneKeyExpression.FieldType.STRING, true, true);
        } else if (original instanceof ThenKeyExpression) {
            List<KeyExpression> children = ((ThenKeyExpression)original).getChildren().stream()
                    .map(LuceneTypeConverter::convert).collect(Collectors.toList());
            return new LuceneThenKeyExpression((LuceneFieldKeyExpression)children.get(0), children);
        } else if (original instanceof NestingKeyExpression) {
            NestingKeyExpression nke = (NestingKeyExpression)original;
            return new NestingKeyExpression((FieldKeyExpression)convert(nke.getParent()), convert(nke.getChild()));
        } else if (original instanceof GroupingKeyExpression) {
            KeyExpression child = convert(((GroupingKeyExpression)original).getChild());
            return new GroupingKeyExpression(child, ((GroupingKeyExpression)original).getGroupedCount());
        } else {
            return original;
        }
    }
}

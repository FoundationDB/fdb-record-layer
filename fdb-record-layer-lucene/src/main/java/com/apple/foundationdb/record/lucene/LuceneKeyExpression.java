/*
 * LuceneField.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionVisitor;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.PrefixableExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface LuceneKeyExpression extends PrefixableExpression {

    /**
     * Types allowed for lucene Indexing. StringKeyMap is not explicitly specifying the values because we expect the
     * child expressions to be lucene expressions and specify it from the below choices.
     */
    enum FieldType {
        STRING,
        INT,
        INT_LIST,
        STRING_KEY_MAP
    }


    /**
     * For nested fields and possibly in the future more validation on fields + types and possibly field names.
     * Other possible things to check would be complexity and number of levels.
     *
     * @param expression the expression to validate
     *
     * @return if the entire expression is validated as lucene compatible
     */
    static boolean validateLucene(KeyExpression expression) {
        if (expression instanceof LuceneKeyExpression) {
            return true;
        }
        if (expression instanceof GroupingKeyExpression) {
            return validateLucene(((GroupingKeyExpression)expression).getWholeKey());
        }
        if (expression instanceof NestingKeyExpression) {
            boolean valid = true;
            for (KeyExpression keyExpression : ((NestingKeyExpression)expression).getChildren()) {
                valid = valid && validateLucene(keyExpression);
            }
            return valid;
        }
        throw new MetaDataException("Unsupported field type, please check allowed lucene field types under LuceneField class", LogMessageKeys.KEY_EXPRESSION, expression);
    }

    static List<LuceneKeyExpression> normalize(KeyExpression expression) {
        return normalize(expression, "");
    }

    // Todo: limit depth of recursion
    static List<LuceneKeyExpression> normalize(KeyExpression expression, @Nullable String givenPrefix) {
        if (expression instanceof LuceneFieldKeyExpression) {
            ((LuceneFieldKeyExpression)expression).prefix(givenPrefix);
            return Lists.newArrayList((LuceneFieldKeyExpression)expression);
        } else if (expression instanceof LuceneThenKeyExpression) {
            if (((LuceneThenKeyExpression)expression).fan()){
                ((LuceneThenKeyExpression)expression).prefix(givenPrefix);
                return Lists.newArrayList((LuceneKeyExpression)expression);
            } else {
                return ((LuceneThenKeyExpression)expression).getChildren().stream().flatMap(
                        e -> normalize(e, givenPrefix).stream()).collect(Collectors.toList());
            }
        } else if (expression instanceof GroupingKeyExpression) {
            return normalize(((GroupingKeyExpression)expression).getWholeKey(), givenPrefix);
        } else if (expression instanceof NestingKeyExpression) {
            String newPrefix = givenPrefix.concat(((NestingKeyExpression)expression).getParent().getFieldName()).concat("_");
            return Lists.newArrayList(normalize(((NestingKeyExpression)expression).getChild(), newPrefix));
        } else if (expression instanceof ThenKeyExpression) {
            return ((ThenKeyExpression)expression).getChildren().stream().flatMap(e -> normalize(e, givenPrefix).stream()).collect(Collectors.toList());
        }
        throw new RecordCoreArgumentException("tried to normalize a non-lucene, non-grouping expression. These are currently unsupported.", LogMessageKeys.KEY_EXPRESSION, expression);
    }

    static Set<String> getPrefixedFieldNames(KeyExpression expression) {
        Set<String> fieldNames = new HashSet<>();
        KeyExpressionVisitor visitor = new KeyExpressionVisitor() {
            @Override
            public KeyExpression visitField(final FieldKeyExpression fke) {
                fieldNames.add(((LuceneFieldKeyExpression)fke).getPrefixedFieldName());
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
                fieldNames.add(nke.getParent().getFieldName());
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
        visitor.visit(expression);
        return fieldNames;
    }

}

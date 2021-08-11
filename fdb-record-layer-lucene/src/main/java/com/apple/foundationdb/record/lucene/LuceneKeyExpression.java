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
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public interface LuceneKeyExpression extends KeyExpression {

    /**
     * Types allowed for lucene Indexing. StringKeyMap is not explicitly specifying the values because we expect the
     * child expressions to be lucene expressions and specify it from the below choices.
     */
    enum FieldType {
        STRING,
        INT,
        LONG,
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

    // Todo: limit depth of recursion
    static List<ImmutablePair<String, LuceneKeyExpression>> normalize(KeyExpression expression, @Nullable String givenPrefix) {
        if (expression instanceof LuceneFieldKeyExpression) {
            return Lists.newArrayList(new ImmutablePair<>(givenPrefix, (LuceneFieldKeyExpression)expression));
        } else if (expression instanceof LuceneThenKeyExpression) {
            if (((LuceneThenKeyExpression)expression).fan()) {
                return Lists.newArrayList(new ImmutablePair<>(givenPrefix, (LuceneKeyExpression)expression));
            } else {
                return ((LuceneThenKeyExpression)expression).getChildren().stream().flatMap(
                        e -> normalize(e, givenPrefix).stream()).collect(Collectors.toList());
            }
        } else if (expression instanceof GroupingKeyExpression) {
            return normalize(((GroupingKeyExpression)expression).getWholeKey(), givenPrefix);
        } else if (expression instanceof NestingKeyExpression) {
            String newPrefix = givenPrefix;
            if (((NestingKeyExpression)expression).getParent().getFanType() == FanType.None) {
                newPrefix = givenPrefix == null ? "" : givenPrefix.concat("_");
                newPrefix = newPrefix.concat(((NestingKeyExpression)expression).getParent().getFieldName());
            }
            return Lists.newArrayList(normalize(((NestingKeyExpression)expression).getChild(), newPrefix));
        } else if (expression instanceof ThenKeyExpression) {
            return ((ThenKeyExpression)expression).getChildren().stream().flatMap(e -> normalize(e, givenPrefix).stream()).collect(Collectors.toList());
        }
        throw new RecordCoreArgumentException("tried to normalize a non-lucene, non-grouping expression. These are currently unsupported.", LogMessageKeys.KEY_EXPRESSION, expression);
    }

    static List<String> listIndexFieldNames(KeyExpression expression) {
        List<ImmutablePair<String, LuceneKeyExpression>> pairs = normalize(expression, null);
        List<String> indexFields = Lists.newArrayList();
        for (ImmutablePair<String, LuceneKeyExpression> pair : pairs) {
            if (pair.right instanceof LuceneFieldKeyExpression) {
                indexFields.add(pair.left != null ?
                                pair.left.concat("_").concat(((LuceneFieldKeyExpression)pair.right).getFieldName()) :
                                ((LuceneFieldKeyExpression)pair.right).getFieldName());
            } else if (pair.right instanceof LuceneThenKeyExpression) {
                if (pair.left != null) {
                    indexFields.add(pair.left);
                }
                KeyExpression primaryKey = ((LuceneThenKeyExpression)pair.right).getPrimaryKey();
                if (primaryKey instanceof FieldKeyExpression) {
                    indexFields.add(((FieldKeyExpression)primaryKey).getFieldName());
                }
            } else {
                if (pair.left != null) {
                    indexFields.add(pair.left);
                }
            }
        }
        return indexFields;
    }
}

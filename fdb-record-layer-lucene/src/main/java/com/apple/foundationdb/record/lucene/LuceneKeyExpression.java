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
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.PrefixableExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
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
    static List<LuceneKeyExpression> normalize(KeyExpression expression, String prefix) {
        if (expression instanceof LuceneFieldKeyExpression) {
            ((LuceneFieldKeyExpression)expression).prefix(prefix);
            return Lists.newArrayList((LuceneFieldKeyExpression)expression);
        } else if (expression instanceof LuceneThenKeyExpression) {
            ((LuceneThenKeyExpression)expression).prefix(prefix);
            if (!((LuceneThenKeyExpression)expression).fan()) return Lists.newArrayList((LuceneKeyExpression)expression);
            return ((LuceneThenKeyExpression)expression).getChildren().stream().flatMap(e -> normalize(e).stream()).collect(Collectors.toList());
        } else if (expression instanceof GroupingKeyExpression) {
            return normalize(((GroupingKeyExpression)expression).getWholeKey(), prefix);
        } else if (expression instanceof NestingKeyExpression) {
            return Lists.newArrayList(normalize(((NestingKeyExpression)expression).getChild(),
                    prefix.concat(((NestingKeyExpression)expression).getParent().getFieldName().concat("_"))));
        } else if (expression instanceof ThenKeyExpression) {
            return ((ThenKeyExpression)expression).getChildren().stream().flatMap(e -> normalize(e).stream()).collect(Collectors.toList());
        }
        throw new RecordCoreArgumentException("tried to normalize a non-lucene, non-grouping expression. These are currently unsupported.", LogMessageKeys.KEY_EXPRESSION, expression);
    }

    static List<String> getPrefixedFieldNames(KeyExpression expression) {
        for (LuceneKeyExpression luceneKeyExpression : normalize(expression)) {
            if (luceneKeyExpression instanceof LuceneFieldKeyExpression) {
                return Lists.newArrayList(((LuceneFieldKeyExpression)luceneKeyExpression).getPrefixedFieldName());
            } else if (luceneKeyExpression instanceof LuceneThenKeyExpression) {
                List<String> names = Lists.newArrayList();
                for (KeyExpression child : ((LuceneThenKeyExpression)luceneKeyExpression).getLuceneChildren()) {
                    names.addAll(getPrefixedFieldNames(child));
                }
                return names;
            } else if (expression instanceof GroupingKeyExpression) {
                return getPrefixedFieldNames(((GroupingKeyExpression)expression).getWholeKey());
            } else if (expression instanceof NestingKeyExpression) {
                // Validation that the top level parent field exists should be enough since we expand
                // and change the field names anyway. This is also important for primay key field locations.
                List<String> names = getPrefixedFieldNames(((NestingKeyExpression)expression).getParent());
                return names;
            }
        }
        return Collections.emptyList();
    }

}

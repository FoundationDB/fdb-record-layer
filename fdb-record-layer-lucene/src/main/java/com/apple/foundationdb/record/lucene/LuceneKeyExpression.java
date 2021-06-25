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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.collect.Lists;

import java.util.List;

public interface LuceneKeyExpression extends KeyExpression {

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
     * @return if the entire expression is validated as lucene compatible
     */
    public boolean validateLucene();

    public static List<LuceneKeyExpression> normalize(KeyExpression keyExpression){
        List<KeyExpression> expressions = keyExpression.normalizeKeyForPositions();
        List<LuceneKeyExpression> luceneKeyExpressions = Lists.newArrayList();
        for (KeyExpression expression : expressions) {
            if (!(expression instanceof LuceneKeyExpression))
                throw new IllegalArgumentException("LuceneKeyExpression has child that is not LuceneKeyExpression type");
            luceneKeyExpressions.add((LuceneKeyExpression)expression);
        }
        return luceneKeyExpressions;
    }



}

/*
 * IndexBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// todo: refactor and move to its own package
public class IndexBuilder {
    private RecordMetaDataProto.Index.Builder indexBuilder = RecordMetaDataProto.Index.newBuilder();
    private List<String> includedFields;
    private List<String> indexedFields;

    public IndexBuilder(String name) {
        indexBuilder = this.indexBuilder.setName(name);
    }

    public IndexBuilder setType(String type) {
        indexBuilder.setType(type);
        return this;
    }

    public IndexBuilder indexFields(List<String> fields) {
        this.indexedFields = fields;
        return this;
    }

    public IndexBuilder includeFields(List<String> includeFields) {
        this.includedFields = includeFields;
        return this;
    }

    public RecordMetaDataProto.Index build() {
        List<KeyExpression> fieldExpressions = indexedFields.stream()
                .map(Key.Expressions::field).collect(Collectors.toList());
        List<KeyExpression> includeExprs = includedFields.stream()
                .map(Key.Expressions::field).collect(Collectors.toList());
        fieldExpressions.add(0, Key.Expressions.recordType());
        KeyExpression rootExpression;
        if (includedFields.isEmpty()) {
            rootExpression = Key.Expressions.concat(fieldExpressions);
        } else {
            List<KeyExpression> allFields = new ArrayList<>(fieldExpressions.size() + includeExprs.size());
            allFields.addAll(fieldExpressions);
            allFields.addAll(includeExprs);
            rootExpression = Key.Expressions.keyWithValue(Key.Expressions.concat(allFields), fieldExpressions.size());
        }
        indexBuilder.setRootExpression(rootExpression.toKeyExpression());
        return indexBuilder.build();
    }

}

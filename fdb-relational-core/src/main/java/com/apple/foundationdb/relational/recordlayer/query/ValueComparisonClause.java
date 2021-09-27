/*
 * ValueComparisonClause.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.WhereClause;

/**
 * Representing a Comparison against a literal value.
 */
public class ValueComparisonClause implements WhereClause {

    public enum ComparisonType {
        EQUALS("="),
        NOT_EQUALS("!="),
        GT(">"),
        GREATER_OR_EQUALS(">="),
        LT("<"),
        LESS_OR_EQUALS("<="),
        LIKE("like"),
        IS("is"),
        IS_NOT("is not");

        private final String textForm;

        ComparisonType(String textForm) {
            this.textForm = textForm;
        }

        @Override
        public String toString() {
            return textForm;
        }
    }

    private final String field;
    private final ComparisonType type;
    private final Object constant;

    public ValueComparisonClause(String field, ComparisonType type, Object constant) {
        this.field = field;
        this.type = type;
        this.constant = constant;
    }

    public Object getComparisonValue() {
        return constant;
    }

    public String getField() {
        return field;
    }

    public ComparisonType getType() {
        return type;
    }
}

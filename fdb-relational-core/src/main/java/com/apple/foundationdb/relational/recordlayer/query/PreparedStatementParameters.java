/*
 * ParserContext.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import java.util.Map;

public class PreparedStatementParameters {

    private Map<Integer, Object> parameters;

    private Map<String, Object> namedParameters;

    private int nextParam = 1;

    public PreparedStatementParameters() {
    }

    public PreparedStatementParameters(Map<Integer, Object> parameters, Map<String, Object> namedParameters) {
        this.parameters = parameters;
        this.namedParameters = namedParameters;
    }

    public Object getNextParameter() {
        Assert.thatUnchecked(parameters != null && parameters.containsKey(nextParam),
                "No value found for parameter " + nextParam,
                ErrorCode.UNDEFINED_PARAMETER);
        return parameters.get(nextParam++);
    }

    public Object getNamedParameter(String name) {
        Assert.thatUnchecked(namedParameters != null && namedParameters.containsKey(name),
                "No value found for parameter " + name,
                ErrorCode.UNDEFINED_PARAMETER);
        return namedParameters.get(name);
    }
}

/*
 * JsonOutputFormatPlus.java
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

package com.apple.foundationdb.relational.cli.sqlline;

import sqlline.JsonOutputFormat;
import sqlline.SqlLine;

/**
 * Add support for Relational STRUCT and ARRAY to JSON processing.
 */
public class JsonOutputFormatPlus extends JsonOutputFormat {
    public JsonOutputFormatPlus(SqlLine sqlLine) {
        super(sqlLine);
    }

    // TODO!!!!
}

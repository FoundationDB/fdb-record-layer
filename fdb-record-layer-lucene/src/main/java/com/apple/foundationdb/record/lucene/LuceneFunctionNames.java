/*
 * LuceneFunctionNames.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

/**
 * Key function names for Lucene indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneFunctionNames {
    public static final String LUCENE_FIELD_NAME = "lucene_field_name";
    public static final String LUCENE_STORED = "lucene_stored";
    public static final String LUCENE_TEXT = "lucene_text";

    private LuceneFunctionNames() {
    }
}

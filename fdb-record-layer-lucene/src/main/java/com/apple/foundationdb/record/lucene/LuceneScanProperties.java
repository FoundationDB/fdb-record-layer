/*
 * LuceneScanProperties.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A light wrapper class that supports passing through a sort expression to the LuceneRecordCursor.
 */
public class LuceneScanProperties extends ScanProperties {

    private final KeyExpression sort;

    public LuceneScanProperties(@Nonnull final ExecuteProperties executeProperties, @Nullable KeyExpression sort, boolean reverse) {
        super(executeProperties, reverse);
        this.sort = sort;
    }

    public KeyExpression getSort() {
        return sort;
    }

}

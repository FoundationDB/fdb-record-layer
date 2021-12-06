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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;

public class LuceneExecuteProperties extends ExecuteProperties {
    ExecutorService service;
    KeyExpression sort;

    public LuceneExecuteProperties(ExecuteProperties properties, ExecutorService service, @Nullable KeyExpression sort){
        super(properties.getSkip(), properties.getReturnedRowLimitOrMax(), properties.getIsolationLevel(), properties.getTimeLimit(), properties.getState(), properties.isFailOnScanLimitReached(), properties.getDefaultCursorStreamingMode());
        this.service = service;
        this.sort = sort;
    }

}

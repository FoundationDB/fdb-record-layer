/*
 * RecordMetaDataProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * A source of {@link RecordMetaData}.
 *
 * If getting meta-data is expensive, implementations should assume that {@link #getRecordMetaData} will be called frequently and cache the result.
 */
@API(API.Status.UNSTABLE)
public interface RecordMetaDataProvider {

    /**
     * Provide an instance of {@link RecordMetaData}.
     * Implementors should assume that this method will be called frequently, so it may
     * be necessary to cache the result if generating the {@link RecordMetaData} is expensive.
     *
     * @return an instance of {@link RecordMetaData}
     */
    @Nonnull
    RecordMetaData getRecordMetaData();
}

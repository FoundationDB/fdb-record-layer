/*
 * PipelineOperation.java
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
 * Kind of asynchronous pipelined operation being performed.
 *
 * This isn't an enum so clients can define more of them for their own pipeline operations.
 *
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.PipelineSizer
 * @see RecordCursor#mapPipelined
 */
@API(API.Status.UNSTABLE)
public class PipelineOperation {
    @Nonnull
    public static final PipelineOperation INDEX_TO_RECORD = new PipelineOperation("INDEX_TO_RECORD");
    @Nonnull
    public static final PipelineOperation KEY_TO_RECORD = new PipelineOperation("KEY_TO_RECORD");
    @Nonnull
    public static final PipelineOperation RECORD_ASYNC_FILTER = new PipelineOperation("RECORD_ASYNC_FILTER");
    @Nonnull
    public static final PipelineOperation INDEX_ASYNC_FILTER = new PipelineOperation("INDEX_ASYNC_FILTER");
    @Nonnull
    public static final PipelineOperation RECORD_FUNCTION = new PipelineOperation("RECORD_FUNCTION");
    @Nonnull
    public static final PipelineOperation RESOLVE_UNIQUENESS = new PipelineOperation("RESOLVE_UNIQUENESS");
    @Nonnull
    public static final PipelineOperation IN_JOIN = new PipelineOperation("IN_JOIN");
    @Nonnull
    public static final PipelineOperation TEXT_INDEX_UPDATE = new PipelineOperation("TEXT_INDEX_UPDATE");
    @Nonnull
    public static final PipelineOperation SYNTHETIC_RECORD_JOIN = new PipelineOperation("SYNTHETIC_RECORD_JOIN");
    @Nonnull
    public static final PipelineOperation UPDATE = new PipelineOperation("UPDATE");
    @Nonnull
    public static final PipelineOperation DELETE = new PipelineOperation("DELETE");
    @Nonnull
    public static final PipelineOperation INSERT = new PipelineOperation("INSERT");

    private final String name;

    public PipelineOperation(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}

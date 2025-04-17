/*
 * IndexingByRecords.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexBuildProto;

import javax.annotation.Nonnull;

/**
 *  The old "IndexingByRecords" indexer was replaced by {@link IndexingMultiTargetByRecords}.
 *  This module is its placeholder.
 */
@API(API.Status.INTERNAL)
public final class IndexingByRecordsLegacyPlaceHolder {

    private IndexingByRecordsLegacyPlaceHolder() {
        // No op
    }

    /**
     * Maintaining a "fake" typeStamp that is being used when detecting a partially built index
     * with no type stamp. The assumption is such indexes where built by the old, removed, indexer.
     * @return typeStamp for the old ByRecords indexer
     */
    @Nonnull
    static IndexBuildProto.IndexBuildIndexingStamp compileIndexingTypeStamp() {
        return
                IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS)
                        .build();
    }
}

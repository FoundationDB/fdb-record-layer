/*
 * QueryResult.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.google.protobuf.Message;

/**
 * QueryResult is the general result that encapsulates the data that is flowing up from plan to consumer. The QueryResult
 * can hold several elements for each result. The elements are opaque from the result perspective, but are known at planning time
 * so that the planner can address them as needed and extract and transfer them from one cursor to another.
 * This class is immutable - all modify operations will cause a new instance to be created with the modified value, leaving
 * the original instance intact. The internal data structure is also immutable.
 */
@API(API.Status.EXPERIMENTAL)
public interface QueryResult {
    @SuppressWarnings("unchecked")
    static <M extends Message> Object unwrapValue(final Object o) {
        final FDBRecord<M> currentFdbRecord = (o instanceof FDBRecord<?>) ? (FDBRecord<M>)o : null;

        if (currentFdbRecord != null) {
            return currentFdbRecord.getRecord();
        } else {
            return o; // this may be null or anything other than a FDBRecord
        }
    }
}

/*
 * IndexOrphanBehavior.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;

/**
 * Provided during index scan operations in which associated records are being retrieved, to indicate
 * what should happen in response to an index entry which has no associated record.  This situation
 * should only arrive in indexes in which the maintenance of a delete is too expensive and must be
 * deferred to some kind of external support job.
 */
@API(API.Status.UNSTABLE)
public enum IndexOrphanBehavior {
    /**
     * Throw an exception when an orphaned entry is hit. This is the default behavior for most
     * indexes in which we never expect orphaned entries.
     */
    ERROR,

    /**
     * Silently ignore the index entry. This would be used when scanning an index that is known to
     * contain (or likely contain) orphaned entries.
     */
    SKIP,

    /**
     * Return the index entry but with no record associated with it. This would most likely be used
     * when implementing the deferred maintenance of the index to recognize when there are orphaned
     * entries and to then take care of cleaning them up.
     */
    RETURN
}

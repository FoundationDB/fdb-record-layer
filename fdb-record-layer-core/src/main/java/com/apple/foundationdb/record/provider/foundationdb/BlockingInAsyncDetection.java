/*
 * BlockingInAsyncDetection.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
 * Indicates whether <code>FDBDatabase.asyncToSync()</code> or <code>FDBRecordContext.asyncToSync()</code> should
 * attempt to detect when it is being called from an asynchronous context and, if so, how it should react to
 * this fact.  When this situation is detected, there are two scenarios that need to be dealt with:
 * <ul>
 *     <li><b>Completed future</b> - the future that was to be waited on is completed. In some code, this is
 *       normal expected behavior; some prior logic ensured that the future was completed and subsequent calls
 *       just "know" they won't block.  Of course, it is possible that there is no such logic and the
 *       future was simply accidentally completed.  As a result there are two options for dealing with
 *       the presence of such completed futures: <code>IGNORE_COMPLETE</code> (silently ignore) or
 *       <code>WARN_COMPLETE</code> (log a warning).</li>
 *     <li><b>Non-complete future</b> - the future that was to be waited on is not complete. There is (typically) no
 *       circumstance in which this is acceptable, so the options to react to this scenario are
 *       <code>EXCEPTION_BLOCKING</code> (throw an exception) or <code>WARN_BLOCKING</code> (log a warning).</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public enum BlockingInAsyncDetection {
    /**
     * The detection of blocking in an asynchronous context is disabled.
     */
    DISABLED(true, false),

    /**
     * When <code>asyncToSync</code> is called in an asynchronous context and the future it is passed would
     * block, an exception will be thrown.
     */
    IGNORE_COMPLETE_EXCEPTION_BLOCKING(true, true),

    /**
     * When <code>asyncToSync</code> is called in an asynchronous context and the future it is passed would
     * block, an exception will be thrown; however, if the future is complete, then a warning will be logged.
     */
    WARN_COMPLETE_EXCEPTION_BLOCKING(false, true),

    /**
     * When <code>asyncToSync</code> is called in an asynchronous context and the future it is passed would
     * block, a warning is logged.
     */
    IGNORE_COMPLETE_WARN_BLOCKING(true, false),

    /**
     * When <code>asyncToSync</code> is called in an asynchronous context, a warning is logged.
     */
    WARN_COMPLETE_WARN_BLOCKING(false, false)
    ;

    private final boolean ignoreComplete;
    private final boolean throwExceptionOnBlocking;

    BlockingInAsyncDetection(boolean ignoreComplete, boolean throwException) {
        this.ignoreComplete = ignoreComplete;
        this.throwExceptionOnBlocking = throwException;
    }

    /**
     * Indicates how to react if the future passed into <code>asyncToSync()</code> was completed. A return
     * value of <code>true</code> indicates that this situation should be ignored, and a value of <code>false</code>
     * indicates that a warning should be logged.
     *
     * @return whether completed future should be ignored
     */
    public boolean ignoreComplete() {
        return ignoreComplete;
    }

    /**
     * Indicates how to react if the future passed into <code>asyncToSync()</code> has not yet been completed. A return
     * value of <code>true</code> indicates that this situation should result in a {@link BlockingInAsyncException},
     * and a value of <code>false</code> indicates that a warning should be logged.
     *
     * @return whether non-completed future should throw an exception
     */
    public boolean throwExceptionOnBlocking() {
        return throwExceptionOnBlocking;
    }
}

/*
 * LuceneLoggerInfoStream.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

/**
 * Record Layer's implementation of {@link InfoStream} that publishes messages as TRACE logs.
 */
@SuppressWarnings("PMD.LoggerIsNotStaticFinal")
public class LuceneLoggerInfoStream extends InfoStream {
    private final Logger loggerForStreamOutput;

    public LuceneLoggerInfoStream(@Nonnull Logger loggerForStreamOutput) {
        this.loggerForStreamOutput = loggerForStreamOutput;
    }

    @Override
    public void message(String component, String message) {
        if (loggerForStreamOutput.isTraceEnabled()) {
            loggerForStreamOutput.trace(KeyValueLogMessage.of(message, LuceneLogMessageKeys.COMPONENT, component));
        }
    }

    @Override
    public boolean isEnabled(String component) {
        return true;
    }

    @Override
    public void close() {
        // Nothing to close
    }
}

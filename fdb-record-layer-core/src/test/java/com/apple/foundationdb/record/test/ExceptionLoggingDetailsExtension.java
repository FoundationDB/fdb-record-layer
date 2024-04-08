/*
 * ConflictExceptionExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.test;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.util.LoggableKeysAndValues;
import com.google.auto.service.AutoService;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Exception handler which log information from test exceptions. This will ensure that we print out any relevant
 * information contained in the log message keys.
 */
@AutoService(Extension.class)
public class ExceptionLoggingDetailsExtension implements TestExecutionExceptionHandler {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionLoggingDetailsExtension.class);

    @Nullable
    private Map<String, Object> collectLogInfo(@Nonnull Throwable throwable) {
        Map<String, Object> keys = null;
        Set<Throwable> seen = new HashSet<>();
        @Nullable Throwable current = throwable;
        while (current != null) {
            if (current instanceof LoggableKeysAndValues<?>) {
                Map<String, Object> logInfo = ((LoggableKeysAndValues<?>)current).getLogInfo();
                if (!logInfo.isEmpty()) {
                    if (keys == null) {
                        keys = new TreeMap<>();
                    }
                    keys.putAll(logInfo);
                }
            }
            current = seen.add(throwable) ? throwable.getCause() : null;
        }
        return keys;
    }

    @Override
    public void handleTestExecutionException(final ExtensionContext extensionContext, final Throwable throwable) throws Throwable {
        Map<String, Object> logInfo = collectLogInfo(throwable);
        if (logInfo != null) {
            KeyValueLogMessage message = KeyValueLogMessage.build("test failure exception details");
            message.addKeysAndValues(logInfo);
            LOGGER.error(message.toString(), throwable);
        }
        throw throwable;
    }
}

/*
 * package-info.java
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

/**
 * Helper classes for logging.
 *
 * <h2>Logging in the Record Layer</h2>
 *
 * <p>
 * All logging is done using <a href="https://www.slf4j.org/">SLF4J</a>, which should be compatible with
 * whatever logging framework the client system uses.
 * </p>
 *
 * <p>
 * All {@link org.slf4j.Logger}s have names starting with <code>com.apple.foundationdb.record</code>.
 * </p>
 *
 * <p>
 * Log messages of any complexity use {@link com.apple.foundationdb.record.logging.KeyValueLogMessage}, which outputs in a standard <code>key=value</code> format.
 * Most of the standard log message keys appear in {@link com.apple.foundationdb.record.logging.LogMessageKeys}.
 * </p>
 *
 * <p>
 * Since record operations are fundamentally asynchronous and makes extensive use of {@link java.util.concurrent.CompletableFuture} composition, log messages for a single operation will
 * often come from multiple thread pool threads. To allow these to be associated with a calling context in the log, the {@link org.slf4j.MDC} is used.
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBDatabase#openContext} takes an optional MDC map, which will be maintained for all work done by the {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext}.
 * </p>
 */
package com.apple.foundationdb.record.logging;

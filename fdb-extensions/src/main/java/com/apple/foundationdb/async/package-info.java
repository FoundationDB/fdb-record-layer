/*
 * package-info.java
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

/**
 * Utility functions for use within an asynchronous environment.
 *
 * <p>
 * The data structures are designed to be compatible for use within a asynchronous environment
 * in that they are all intended to be non-blocking. They are used within the
 * <a href="https://foundationdb.github.io/fdb-record-layer/">Record Layer</a> core library
 * to implement some of the more advanced secondary index types.
 * </p>
 *
 * <p>
 * The utility functions within the {@link com.apple.foundationdb.async.MoreAsyncUtil} class are
 * of a kind with the functions in the {@link com.apple.foundationdb.async.AsyncUtil} class within the
 * FoundationDB Java bindings. They encapsulate a few common uses of the
 * {@link java.util.concurrent.CompletableFuture} class.
 * </p>
 */
package com.apple.foundationdb.async;

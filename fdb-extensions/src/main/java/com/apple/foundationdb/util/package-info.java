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
 * Utilities for logging and exception handling.
 * All exceptions in the <a href="https://foundationdb.github.io/fdb-record-layer/">Record Layer</a>
 * project are descendants of {@link com.apple.foundationdb.util.LoggableException} class. This
 * class allows the user to attach arbitrary keys and values to an {@link java.lang.Exception} when
 * thrown.
 */
package com.apple.foundationdb.util;

/*
 * package-info.java
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

/**
 * Utilities for interacting with the system keyspace. In FoundationDB, those are keys beginning with the literal byte
 * {@code 0xff}. These keys are used by the system to store configuration information as well as to provide endpoints
 * for executing special functions (e.g., locking the database).
 *
 * <p>
 * The contents of this package should generally be considered experimental. There is also a project to expose these
 * special keys in a more systematic way. See:
 * <a href="https://github.com/apple/foundationdb/blob/master/design/special-key-space.md">Special key space</a>.
 * </p>
 */
package com.apple.foundationdb.system;

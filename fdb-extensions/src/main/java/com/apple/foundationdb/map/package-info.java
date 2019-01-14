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
 * A sorted durable associative array with pluggable serialization.
 *
 * <p>
 * The main class in this package is the {@link com.apple.foundationdb.map.BunchedMap} class. This
 * presents an API that is similar to a standard sorted associated array or map. It differs
 * from other such implementations in that it attempts to consolidate multiple entries in
 * the final map into each FoundationDB key-value pair. This allows the data structure to reduce its
 * total footprint by spreading its data across fewer keys than a na&iuml;ve implementation might.
 * The other classes are used to support the {@code BunchedMap} class in various ways.
 * </p>
 */
package com.apple.foundationdb.map;

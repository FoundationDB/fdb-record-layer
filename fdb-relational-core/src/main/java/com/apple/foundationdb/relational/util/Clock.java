/*
 * Clock.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

/**
 * A simple interface for representing a Clock logically.
 *
 * This is useful whenever you might want to change the behavior of "time" in logical ways (for example,
 * by using a monotonic counter instead of an actual clock) without changing the algorithms that use it.
 * <p>
 * There are a bunch of interfaces like this in our library set, having our own is useful for dependency management,
 * otherwise it would just be a waste of text.
 */
public interface Clock {

    /**
     * Read the clock's logical time, with nanosecond units.
     *
     * @return the clock's logical "time", with nanosecond units.
     */
    long readNanos();
}

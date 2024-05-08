/*
 * package-info.java
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

/**
 * Classes used for utility classes for handling pairs of elements. It includes a main
 * {@link com.apple.foundationdb.record.util.pair.Pair} interface and a few implementations to
 * support null checks and comparison operations.
 *
 * <p>
 * These classes are {@link com.apple.foundationdb.annotation.API.Status#INTERNAL INTERNAL}
 * and should only be used within the Record Layer code base, but they are not intended to be used by
 * consumers. The general advice is also that, in general, code writers should prefer complex
 * classes with named instead of unnamed pairs (or higher order varieties).
 * </p>
 *
 * @see com.apple.foundationdb.record.util.pair.Pair
 */
package com.apple.foundationdb.record.util.pair;

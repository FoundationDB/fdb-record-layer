/*
 * ZeroCopyByteString.java
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

package com.google.protobuf;

import javax.annotation.Nonnull;

/**
 * Use this as a way to use existing byte arrays when converting to ByteString for
 * protobuf conversion.  Critical to remember that if the backing byte[] mutates, so does your
 * ByteString.
 *
 */
public class ZeroCopyByteString {

    public static ByteString wrap(@Nonnull byte[] bytes) {
        return UnsafeByteOperations.unsafeWrap(bytes);
    }

}

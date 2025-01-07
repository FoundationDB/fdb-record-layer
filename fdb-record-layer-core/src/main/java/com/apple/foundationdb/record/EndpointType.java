/*
 * EndpointType.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * The type of one of the endpoints of a {@link TupleRange}.
 * <p>
 * The basic types are inclusive (range includes endpoint {@link com.apple.foundationdb.tuple.Tuple} value) and exclusive (range does not include endpoint {@code Tuple}).
 * Special types denote the start and end of the whole subspace (for which the endpoint {@code Tuple} does not matter).
 * There is also a special type for the range of values where a string element of a {@code Tuple} begins with a given string.
 * </p>
 */
@API(API.Status.UNSTABLE)
public enum EndpointType {
    TREE_START,
    TREE_END,
    RANGE_INCLUSIVE,
    RANGE_EXCLUSIVE,
    PREFIX_STRING,
    CONTINUATION;

    @Nonnull
    public String toString(boolean high) {
        switch (this) {
            case TREE_START:
                return "<";
            case TREE_END:
                return ">";
            case RANGE_INCLUSIVE:
                return (high) ? "]" : "[";
            case RANGE_EXCLUSIVE:
                return (high) ? ")" : "(";
            case PREFIX_STRING:
                return (high) ? "}" : "{";
            default:
                return "?";
        }
    }
}

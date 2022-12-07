/*
 * MapUtils.java
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

package com.apple.foundationdb.record.util;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * Utility functions for interacting with protobuf.
 */
public class ProtoUtils {

    private ProtoUtils() {
    }

    /**
     * Generates a JVM-wide unique type name.
     * @return a unique type name.
     */
    public static String uniqueTypeName() {
        return uniqueName("__type__");
    }

    /**
     * Generates a JVM-wide unique correlation name.
     * @return a unique type name.
     */
    public static String uniqueCorrelationName() {
        return uniqueName("__corr__");
    }

    private static String uniqueName(String prefix) {
        final var safeUuid = UUID.randomUUID().toString().replace('-', '_');
        return "__type__" + safeUuid;
    }
}

/*
 * SerializationKeyManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;

import java.security.Key;
import java.util.Random;

/**
 * An interface between encrypting serialization, such as {@link TransformedRecordSerializerJCE}, and a source of keys
 * with associated cipher algorithms. Each key is identified by a unique <em>key number</em>, which is persisted in
 * the serialization so that the key can be recovered at deserialization time.
 */
@API(API.Status.EXPERIMENTAL)
public interface SerializationKeyManager {
    /**
     * Get the key number to be used for <em>serializing</em> a record.
     * Typically, this would be the <em>latest</em> key.
     * @return the key number to use
     */
    int getSerializationKey();

    /**
     * Get the key with the given key number.
     * @param keyNumber the unique key identifier
     * @return the cipher used with this key
     */
    Key getKey(int keyNumber);

    /**
     * Get the name of the cipher used with the given key number.
     * @param keyNumber the unique key identifier
     * @return the cipher used with this key
     */
    String getCipher(int keyNumber);

    /**
     * Get a random generator to fill IVs when encrypting.
     * Normally this would be a {@link java.security.SecureRandom} and would not depend on the key.
     */
    // TODO: Perhaps it would be better to have the KM give out an IvParameterSpec or something?
    //  Maybe wait until we have another algorithm that's different enough.
    Random getRandom(int keyNumber);
}

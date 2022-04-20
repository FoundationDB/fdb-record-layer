/*
 * KeySpaceExtension.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.function.Supplier;

/**
 * Convenience extension for adding a simple, pre-built KeySpace to unit tests.
 */
public class KeySpaceExtension implements BeforeAllCallback {
    private final Supplier<KeySpace> keySpaceSupplier;
    private KeySpace keySpace;

    public KeySpaceExtension() {
        this(KeySpaceExtension::getSimpleKeySpace);
    }

    public KeySpaceExtension(Supplier<KeySpace> keySpaceSupplier) {
        this.keySpaceSupplier = keySpaceSupplier;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        keySpace = keySpaceSupplier.get();
    }

    public KeySpace getKeySpace() {
        return keySpace;
    }

    private static KeySpace getSimpleKeySpace() {
        /*
         * A simple KeySpace of the form
         * /
         *  |- dbid(string)
         *      |- schema (string)
         *  |- catalog (null)
         */
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        KeySpaceDirectory schemaDir = new KeySpaceDirectory("schema", KeySpaceDirectory.KeyType.STRING);
        dbDirectory.addSubdirectory(schemaDir);
        KeySpaceDirectory catalogDirectory = new KeySpaceDirectory("catalog", KeySpaceDirectory.KeyType.NULL);
        return new KeySpace(dbDirectory, catalogDirectory);
    }
}

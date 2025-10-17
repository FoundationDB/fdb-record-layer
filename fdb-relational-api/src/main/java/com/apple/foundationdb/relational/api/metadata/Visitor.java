/*
 * Visitor.java
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

package com.apple.foundationdb.relational.api.metadata;

import javax.annotation.Nonnull;

/**
 * A visitor interface for the {@link Metadata}.
 * This visitor allows us to design algorithms that work with metadata artifacts is isolation of metadata API.
 * One use case of it would be for example to write deterministic serialisation and deserialisation logic that
 * requires traversing the metadata more or less in the same topological order of its nodes.
 */
public interface Visitor {

    default void visit(@Nonnull final Metadata metadata) {
        throw new RuntimeException("unexpected");
    }

    void visit(@Nonnull Table table);

    void visit(@Nonnull Column column);

    void startVisit(@Nonnull SchemaTemplate schemaTemplate);

    void visit(@Nonnull SchemaTemplate schemaTemplate);

    void finishVisit(@Nonnull SchemaTemplate schemaTemplate);

    void visit(@Nonnull Schema schema);

    void visit(@Nonnull Index index);

    void visit(@Nonnull InvokedRoutine invokedRoutine);

    void visit(@Nonnull View view);
}

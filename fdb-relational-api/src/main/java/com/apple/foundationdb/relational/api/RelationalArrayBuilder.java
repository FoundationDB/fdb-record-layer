/*
 * RelationalArrayBuilder.java
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

package com.apple.foundationdb.relational.api;

import java.sql.SQLException;

/**
 * Builder for {@link RelationalArray}.
 * This Interface is for implementation by a <a href="https://refactoring.guru/design-patterns/builder">Builder</a>.
 * RelationalArrays only can have {@link RelationalStruct}s as array elements currently.
 */
// Does NOT inherit from RelationalArray; it is complicated. Trying to keep things simple here at least at first.
// TODO: Add handling of primitive types too.
public interface RelationalArrayBuilder {
    /**
     * Build a {@link RelationalArray}.
     * @return A 'built' RelationalStruct instance.
     */
    RelationalArray build();

    RelationalArrayBuilder addStruct(RelationalStruct struct) throws SQLException;
}

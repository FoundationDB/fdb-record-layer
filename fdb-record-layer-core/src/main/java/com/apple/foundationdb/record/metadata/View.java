/*
 * View.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaDataProto;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a SQL view definition stored in the catalog.
 * Views are stored as raw SQL text and compiled lazily when referenced in queries.
 * This class is used for serialization/deserialization of view definitions.
 */
public class View {

    @Nonnull
    private final String name;

    @Nonnull
    private final String definition;

    public View(@Nonnull final String name, @Nonnull final String definition) {
        this.name = name;
        this.definition = definition;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public String getDefinition() {
        return definition;
    }

    @Nonnull
    public RecordMetaDataProto.PView toProto() {
        return RecordMetaDataProto.PView.newBuilder()
                .setName(name)
                .setDefinition(definition)
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final View that = (View) o;
        return Objects.equals(name, that.name) && Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, definition);
    }

    @Override
    public String toString() {
        return "View{" +
                "name='" + name + '\'' +
                ", definition='" + definition + '\'' +
                '}';
    }

    @Nonnull
    public static View fromProto(@Nonnull final RecordMetaDataProto.PView sqlView) {
        return new View(sqlView.getName(), sqlView.getDefinition());
    }
}

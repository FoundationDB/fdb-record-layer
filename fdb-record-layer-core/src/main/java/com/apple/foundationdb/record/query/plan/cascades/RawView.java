/*
 * RawSqlView.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a SQL view definition stored in the catalog.
 * Views are stored as raw SQL text and compiled lazily when referenced in queries.
 * This class is used for serialization/deserialization of view definitions.
 */
public class RawView extends View {

    @Nonnull
    private final String viewName;

    @Nonnull
    private final String definition;

    public RawView(@Nonnull final String viewName, @Nonnull final String definition) {
        this.viewName = viewName;
        this.definition = definition;
    }

    @Nonnull
    @Override
    public String getName() {
        return viewName;
    }

    @Nonnull
    public String getDefinition() {
        return definition;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.PView toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return RecordMetaDataProto.PView.newBuilder()
                .setRawView(RecordMetaDataProto.PRawView.newBuilder()
                    .setName(viewName)
                    .setDefinition(definition)
                    .build())
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
        final RawView that = (RawView) o;
        return Objects.equals(viewName, that.viewName) && Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(viewName, definition);
    }

    @Override
    public String toString() {
        return "PRawView{" +
                "viewName='" + viewName + '\'' +
                ", definition='" + definition + '\'' +
                '}';
    }

    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<RecordMetaDataProto.PRawView, RawView> {
        @Nonnull
        @Override
        public Class<RecordMetaDataProto.PRawView> getProtoMessageClass() {
            return RecordMetaDataProto.PRawView.class;
        }

        @Nonnull
        @Override
        public RawView fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final RecordMetaDataProto.PRawView sqlView) {
            return new RawView(sqlView.getName(), sqlView.getDefinition());
        }
    }
}

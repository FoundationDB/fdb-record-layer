/*
 * PlanDeserializer.java
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
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * Interface that needs to be implemented separately for every class that is {@link PlanSerializable}.
 * @param <M> the message that serializes {@code T}
 * @param <T> the object that is serialized by an instance of {@code M}
 */
@API(API.Status.UNSTABLE)
public interface PlanDeserializer<M extends Message, T> {
    /**
     * Returns {@code M.class}.
     * @return {@code M.class}
     */
    @Nonnull
    Class<M> getProtoMessageClass();

    /**
     * Dispatch method to transform from {@code M} to {@code T}.
     * @param serializationContext the serialization context (and by extension the
     *        {@link com.apple.foundationdb.record.query.plan.serialization.PlanSerializationRegistry}) that is being
     *        used.
     * @param message the protobuf message of type {@code M}
     * @return an instance of type {@code T}
     */
    @Nonnull
    T fromProto(@Nonnull PlanSerializationContext serializationContext, @Nonnull M message);
}

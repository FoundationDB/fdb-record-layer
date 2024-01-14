/*
 * PlanSerializable.java
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
 * Base interface to indicate that a java class is reachable through a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} and therefore needs o be capable of
 * serialization/deserialization. In addition to the {@link #toProto(PlanSerializationContext)} defined here
 * each implementor also needs to provide a {@code public static fromProto(PlanSerializationContext, PClass)}
 * method that we can dynamically dispatch to when deserializing a proto message.
 */
@API(API.Status.UNSTABLE)
public interface PlanSerializable {
    @Nonnull
    Message toProto(@Nonnull PlanSerializationContext serializationContext);
}

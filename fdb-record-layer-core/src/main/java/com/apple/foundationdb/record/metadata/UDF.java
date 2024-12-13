/*
 * UDF.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;

import javax.annotation.Nonnull;

public class UDF {
    @Nonnull private final String udfName;
    @Nonnull
    private final Value value;
    @Nonnull
    private final Value argumentValue;

    public UDF(@Nonnull String udfName, @Nonnull Value value, @Nonnull Value argumentValue) {
        this.udfName = udfName;
        this.value = value;
        this.argumentValue = argumentValue;
    }

    @Nonnull
    public String getUdfName() {
        return udfName;
    }

    @Nonnull
    public Value getValue() {return value;}

    @Nonnull
    public Value getArgumentValue() {return argumentValue;}

    @Nonnull
    public RecordMetaDataProto.UDF toProto() {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        return RecordMetaDataProto.UDF.newBuilder()
                .setName(udfName)
                .setValue(value.toValueProto(serializationContext))
                .setArgumentValue(argumentValue.toValueProto(serializationContext))
                .build();
    }
}

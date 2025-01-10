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
import com.apple.foundationdb.record.query.plan.cascades.MacroFunction;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;

import javax.annotation.Nonnull;

/**
 * Defines a User-defined-function.
 */
public class Udf {
    @Nonnull
    private final MacroFunction macroFunction;

    public Udf(@Nonnull MacroFunction functionValue) {
        this.macroFunction = functionValue;
    }

    @Nonnull
    public MacroFunction getMacroFunction() {
        return macroFunction;
    }

    @Nonnull
    public String getFunctionName() {return macroFunction.getFunctionName();}

    @Nonnull
    public RecordMetaDataProto.Udf toProto() {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        return RecordMetaDataProto.Udf.newBuilder()
                .setFunctionValue(macroFunction.toProto(serializationContext))
                .build();
    }
}

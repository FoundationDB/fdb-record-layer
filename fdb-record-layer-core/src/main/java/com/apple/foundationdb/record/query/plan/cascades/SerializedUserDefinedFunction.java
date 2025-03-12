/*
 * SerializedUserDefinedFunction.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Functions that are 1) can be evaluated against a number of arguments; 2) defined by users; 3) serialized to {@link com.apple.foundationdb.record.RecordMetaDataProto.MetaData}
 * Right now we don't have namespacing rules to separate UserDefinedFunction and BuiltInFunction, so theoretically there could be a naming collision.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class SerializedUserDefinedFunction extends CatalogedFunction {

    public SerializedUserDefinedFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes) {
        super(functionName, parameterTypes, null);
    }

    @Nonnull
    public abstract RecordMetaDataProto.UserDefinedFunction toProto(@Nonnull PlanSerializationContext serializationContext);
}

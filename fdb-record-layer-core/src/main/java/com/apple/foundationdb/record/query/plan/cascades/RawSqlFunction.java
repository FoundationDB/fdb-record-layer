/*
 * RawSqlFunction.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class RawSqlFunction extends UserDefinedFunction {

    @Nonnull
    private final String definition;

    public RawSqlFunction(@Nonnull final String functionName, @Nonnull final String description) {
        super(functionName, ImmutableList.of());
        this.definition = description;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.PUserDefinedFunction toProto() {
        final var builder = RecordMetaDataProto.PRawSqlFunction.newBuilder();
        builder.setName(functionName).setDefinition(definition);
        return RecordMetaDataProto.PUserDefinedFunction.newBuilder()
                .setSqlFunction(builder.build())
                .build();
    }

    @Nonnull
    @Override
    public Typed encapsulate(@Nonnull final List<? extends Typed> arguments) {
        throw new RecordCoreException("attempt to encapsulate raw sql function");
    }

    @Nonnull
    @Override
    public Typed encapsulate(@Nonnull final Map<String, ? extends Typed> namedArguments) {
        throw new RecordCoreException("attempt to encapsulate raw sql function");
    }

    @Nonnull
    public String getDefinition() {
        return definition;
    }

    @Nonnull
    public static RawSqlFunction fromProto(@Nonnull final RecordMetaDataProto.PRawSqlFunction sqlFunction) {
        return new RawSqlFunction(sqlFunction.getName(), sqlFunction.getDefinition());
    }
}

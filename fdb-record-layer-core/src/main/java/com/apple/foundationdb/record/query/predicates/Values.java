/*
 * Values.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.record.query.plan.temp.OrdinalFieldValue;
import com.apple.foundationdb.record.query.plan.temp.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

public class Values {
    @Nonnull
    public static List<? extends Value> deconstructRecord(@Nonnull Value recordValue) {
        Verify.verify(recordValue.getResultType().getTypeCode() == Type.TypeCode.RECORD);
        Verify.verify(recordValue.getResultType() instanceof Type.Record);
        final Type.Record resultType = (Type.Record)recordValue.getResultType();

        if (recordValue instanceof RecordConstructorValue) {
            final var recordConstructorValue = (RecordConstructorValue)recordValue;
            final List<? extends Value> children = ImmutableList.copyOf(recordConstructorValue.getChildren());
            Verify.verify(Objects.requireNonNull(resultType.getFields()).size() == children.size());
            return children;
        }

        final List<Type.Record.Field> fields = Objects.requireNonNull(resultType.getFields());
        final ImmutableList.Builder<Value> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < fields.size(); i++) {
            resultBuilder.add(OrdinalFieldValue.of(recordValue, i));
        }
        return resultBuilder.build();
    }
}

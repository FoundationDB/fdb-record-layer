/*
 * Values.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Values {
    @Nonnull
    public static Value mergeTuples(@Nonnull final List<? extends Value> values) {
        Verify.verify(!values.isEmpty());

        if (values.size() == 1) {
            return values.get(0);
        } else {
            final ImmutableList.Builder<Pair<? extends Value, Optional<String>>> childrenAndNamesBuilder = ImmutableList.builder();
            for (final Value value : values) {
                final Type type = value.getResultType();

                if (type.getTypeCode() != Type.TypeCode.TUPLE) {
                    childrenAndNamesBuilder.add(Pair.of(value, Optional.empty()));
                } else {
                    final Type.Record recordType = (Type.Record)type;
                    final List<? extends Value> elementValues = Type.Record.deconstructTuple(value);
                    final List<Type.Record.Field> fields = Objects.requireNonNull(recordType.getFields());
                    Verify.verify(elementValues.size() == fields.size());
                    for (int i = 0, fieldsSize = fields.size(); i < fieldsSize; i++) {
                        final Type.Record.Field field = fields.get(i);
                        childrenAndNamesBuilder.add(Pair.of(elementValues.get(i), field.getFieldNameOptional()));
                    }
                }
            }
            return TupleConstructorValue.of(childrenAndNamesBuilder.build());
        }
    }
}

/*
 * Atom.java
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

package com.apple.foundationdb.record.query.plan.temp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

public interface Typed {
    @Nonnull
    Type getResultType();
    
    default <T extends Typed> Optional<T> narrowMaybe(@Nonnull final Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    String explain(@Nonnull final Formatter formatter);

    class TypedLiteral implements Typed {
        @Nonnull
        private final Type resultType;

        @Nullable
        private final Object value;

        public TypedLiteral(@Nonnull final Type.TypeCode resultTypeCode, @Nullable final Object value) {
            this.resultType = Type.primitiveType(resultTypeCode);
            this.value = value;
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return resultType;
        }

        @Nonnull
        @Override
        public String explain(@Nonnull final Formatter formatter) {
            throw new UnsupportedOperationException("should not be called");
        }

        @Nullable
        public Object getValue() {
            return value;
        }
    }
}

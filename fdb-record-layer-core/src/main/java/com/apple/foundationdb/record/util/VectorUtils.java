/*
 * VectorUtils.java
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

package com.apple.foundationdb.record.util;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;

public class VectorUtils {
    public static int getVectorPrecision(@Nonnull final RealVector vector) {
        if (vector instanceof DoubleRealVector) {
            return 64;
        }
        if (vector instanceof FloatRealVector) {
            return 32;
        }
        if (vector instanceof HalfRealVector) {
            return 16;
        }
        throw new RecordCoreException("unexpected vector type " + vector.getClass());
    }

    @Nonnull
    public static RealVector parseVector(@Nonnull final ByteString byteString, @Nonnull final Type.Vector vectorType) {
        final var precision = vectorType.getPrecision();
        return parseVector(byteString, precision);
    }

    @Nonnull
    public static RealVector parseVector(@Nonnull final ByteString byteString, int precision) {
        if (precision == 16) {
            return HalfRealVector.fromBytes(byteString.toByteArray());
        }
        if (precision == 32) {
            return FloatRealVector.fromBytes(byteString.toByteArray());
        }
        if (precision == 64) {
            return DoubleRealVector.fromBytes(byteString.toByteArray());
        }
        throw new RecordCoreException("unexpected vector precision " + precision);
    }


}

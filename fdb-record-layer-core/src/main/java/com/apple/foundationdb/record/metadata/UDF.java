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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;

public class UDF {
    @Nonnull private final String udfName;
    @Nonnull private final KeyExpression keyExpression;

    public UDF(@Nonnull String udfName, @Nonnull KeyExpression keyExpression) {
        this.udfName = udfName;
        this.keyExpression = keyExpression;
    }

    @Nonnull
    public String getUdfName() {
        return udfName;
    }

    @Nonnull
    public KeyExpression getKeyExpression() {
        return keyExpression;
    }

    public RecordMetaDataProto.UDF toProto() {
        return RecordMetaDataProto.UDF.newBuilder()
                .setFunction(RecordMetaDataProto.Function.newBuilder()
                        .setName(udfName))
                .build();
    }
}

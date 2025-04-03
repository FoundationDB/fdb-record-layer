/*
 * DefaultValue.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;

import javax.annotation.Nonnull;

/**
 * Sentinel {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} used to represent SQL {@code DEFAULT}
 * argument.
 */
public class DefaultValue extends ThrowsValue  {
    public DefaultValue(@Nonnull final Type resultType) {
        super(resultType);
    }
}

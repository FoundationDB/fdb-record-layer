/*
 * OrderedCollectionTypeContract.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.options;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;

/**
 * Ordered ({@code List}) version of {@link CollectionContract}.
 * @param <T> the type parameter of the collection
 */
public class OrderedCollectionContract<T> extends CollectionContract<T> {
    public OrderedCollectionContract(@Nonnull TypeContract<T> elementContract) {
        super(elementContract);
    }

    @Override
    public void validate(final Options.Name name, final Object value) throws SQLException {
        if (!(value instanceof List<?>)) {
            throw new SQLException("Option " + name + " should be of a list type instead of " + value.getClass().getName(), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
        super.validate(name, value);
    }

    // No need to override fromString, as that already returns a list in value element order.
}

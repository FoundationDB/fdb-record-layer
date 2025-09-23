/*
 * CollectionTypeContract.java
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

package com.apple.foundationdb.relational.api.options;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Option contract that ensures that all elements of a collection match some contract.
 * The contract is only valid if the elements of the given option are (1) some kind
 * of collection type and (2) every element in the collection satisfies the "element
 * contract" over which the contract is defined. For example, the child contract
 * may be that each element is of a given type or within a particular range.
 * @param <T> the type parameter of the collection
 */
public class CollectionContract<T> implements OptionContract, OptionContractWithConversion<Collection<T>> {
    @Nonnull
    private final TypeContract<T> elementContract;

    public CollectionContract(@Nonnull TypeContract<T> elementContract) {
        this.elementContract = elementContract;
    }

    @Override
    public void validate(final Options.Name name, final Object value) throws SQLException {
        if (!(value instanceof Collection<?>)) {
            throw new SQLException("Option " + name + " should be of a collection type instead of " + value.getClass().getName(), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
        try {
            Collection<?> collectionValue = (Collection<?>)value;
            for (Object element : collectionValue) {
                elementContract.validate(name, element);
            }
        } catch (SQLException err) {
            throw new SQLException("Element of collection option " + name + " violated contract: " + err.getMessage(), err.getSQLState(), err);
        }
    }

    @Nullable
    @Override
    public Collection<T> fromString(final String valueAsString) throws SQLException {
        final List<T> results = new ArrayList<>(); // not null-phobic
        for (final String split : valueAsString.split(",")) {
            final String trimmedElementString = split.trim();
            results.add(elementContract.fromString(trimmedElementString));
        }
        return Collections.unmodifiableList(results);
    }
}

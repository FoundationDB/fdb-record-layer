/*
 * RangeContract.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

public final class RangeContract<T extends Comparable<T>> implements OptionContract {
    private final T min;
    private final T max;

    private RangeContract(T min, T max) {
        if (min == null) {
            throw new IllegalArgumentException("RangeContract: Min is null");
        }
        this.min = min;
        if (max == null) {
            throw new IllegalArgumentException("RangeContract: Max is null");
        }
        if (min.compareTo(max) > 0) {
            throw new IllegalArgumentException("RangeContract: Min is not <= Max");
        }
        this.max = max;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void validate(Options.Name name, Object value) throws SQLException {
        T val = (T) value;
        if (min.compareTo(val) > 0 || max.compareTo(val) < 0) {
            throw new SQLException("Option " + name + " should be in range [" + min + ", " + max + "] but is " + value, ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    @Nonnull
    public static <T extends Comparable<T>> RangeContract of(@Nonnull final T min, @Nonnull final T max) {
        return new RangeContract(min, max);
    }
}

/*
 * DoubleValueOrParameter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.spatial.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A X / Y coordinate value expressed as a constant value or the name of a parameter.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class DoubleValueOrParameter implements PlanHashable {
    /**
     * Get the current value from the query bindings.
     * @param context the query context
     * @return a double value or {@code null}.
     */
    public abstract Double getValue(@Nonnull EvaluationContext context);

    /**
     * Get a coordinate for a constant value.
     * @param value the coordinate value
     * @return a new coordinate using the given value
     */
    @Nonnull
    public static DoubleValueOrParameter value(double value) {
        return new DoubleValue(value);
    }

    /**
     * Get a coordinate for a parameterized value.
     * @param parameter the parameter name
     * @return a new coordinate using the given parameter
     */
    @Nonnull
    public static DoubleValueOrParameter parameter(@Nonnull String parameter) {
        return new DoubleParameter(parameter);
    }

    static class DoubleValue extends DoubleValueOrParameter {
        private final double value;

        DoubleValue(double value) {
            this.value = value;
        }

        @Override
        public Double getValue(@Nonnull EvaluationContext context) {
            return value;
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return Double.hashCode(value);
        }

        @Override
        public String toString() {
            return Double.toString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleValue that = (DoubleValue)o;
            return Double.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    static class DoubleParameter extends DoubleValueOrParameter {
        @Nonnull
        private final String parameter;

        DoubleParameter(@Nonnull String parameter) {
            this.parameter = parameter;
        }

        @Override
        public Double getValue(@Nonnull EvaluationContext context) {
            return (Double)context.getBinding(parameter);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return parameter.hashCode();
        }

        @Override
        public String toString() {
            return "$" + parameter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DoubleParameter that = (DoubleParameter)o;
            return parameter.equals(that.parameter);
        }

        @Override
        public int hashCode() {
            return parameter.hashCode();
        }
    }
}

/*
 * UnboundParameter.java
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

package com.apple.foundationdb.relational.yamltests.command.parameterinjection;

import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * {@link UnboundParameter} is a type of {@link Parameter} that does not have a value. Instead, these are the
 * parameters that define "how" the value can be produced from the given the arguments. This is done through
 * {@link Parameter#bind} operation.
 */
public abstract class UnboundParameter implements Parameter {

    @Override
    public boolean isUnbound() {
        return true;
    }

    /**
     * Invalid operation since this parameter is not bound.
     */
    @Override
    @Nullable
    public Object getSqlObject(@Nullable Connection connection) {
        ensureBoundedness();
        return null;
    }

    /**
     * Invalid operation since this parameter is not bound.
     */
    @Nonnull
    @Override
    public String getSqlText() {
        ensureBoundedness();
        return "";
    }

    /**
     * Given a set of {@link Parameter}s, {@link RandomSetParameter} chooses one of them and return it bound.
     */
    public static class RandomSetParameter extends UnboundParameter {
        @Nonnull
        List<Parameter> items;

        public RandomSetParameter(@Nonnull List<Parameter> items) {
            this.items = items;
        }

        @Nonnull
        @Override
        public Parameter bind(@Nonnull Random random) {
            var idx = random.ints(1, 0, items.size()).findFirst().orElseThrow();
            return items.get(idx).bind(random);
        }

        @Override
        public String toString() {
            return "unbounded:randomSet(" + items.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
        }
    }

    /**
     * Returns a random element between the given lowerBound and upperBound. Applicable for only {@link Integer},
     * {@link Double}, {@link Float} and {@link Long} parameters.
     */
    public static class RandomRangeParameter extends UnboundParameter {
        @Nullable
        Parameter lowerBound;
        @Nonnull
        Parameter upperBound;

        public RandomRangeParameter(@Nonnull Parameter upperBound) {
            this(null, upperBound);
        }

        public RandomRangeParameter(@Nullable Parameter lowerBound, @Nonnull Parameter upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Nonnull
        private static Number bindAndVerifyNumberParameter(@Nullable Parameter parameter, @Nonnull Random random) {
            if (parameter == null) {
                return 0;
            }
            var bound = parameter.bind(random);
            Assert.thatUnchecked(bound instanceof PrimitiveParameter, "Expecting a Primitive or Random{Range/Set} Parameter");
            var sqlObject = ((PrimitiveParameter) bound).getSqlObject(null);
            Assert.thatUnchecked(sqlObject instanceof Integer || sqlObject instanceof Long || sqlObject instanceof Double, "The expected bound parameter of type Double, Integer or Long");
            return (Number) sqlObject;
        }

        @Override
        @Nonnull
        public Parameter bind(@Nonnull Random random) {
            final var lb = bindAndVerifyNumberParameter(lowerBound, random);
            final var ub = bindAndVerifyNumberParameter(upperBound, random);
            if (lb instanceof Double || ub instanceof Double) {
                Assert.thatUnchecked(lb.doubleValue() < ub.doubleValue(), "The lower bound parameter should be < the upperbound parameter");
                return new PrimitiveParameter(random.doubles(1, lb.doubleValue(), ub.doubleValue()).findFirst().orElseThrow());
            } else if (lb instanceof Long || ub instanceof Long) {
                Assert.thatUnchecked(lb.longValue() < ub.longValue(), "The lower bound parameter should be < the upperbound parameter");
                return new PrimitiveParameter(random.longs(1, lb.longValue(), ub.longValue()).findFirst().orElseThrow());
            } else {
                Assert.thatUnchecked(lb.intValue() < ub.intValue(), "The lower bound parameter should be < the upperbound parameter");
                return new PrimitiveParameter(random.ints(1, (int) lb, (int) ub).findFirst().orElseThrow());
            }
        }

        @Override
        public String toString() {
            return "unbounded:randomRange(lowerBound: " + lowerBound + ", upperBound: " + upperBound + ")";
        }
    }

    /**
     * Given an element {@link Parameter} and a multiplicity {@link Parameter}, {@link ElementMultiplicityListParameter}
     * evaluates the multiplicity and returns a list of "bound" element repeated multiplicity number of time.
     */
    public static class ElementMultiplicityListParameter extends UnboundParameter {
        @Nonnull
        Parameter element;
        @Nonnull
        Parameter multiplicity;

        public ElementMultiplicityListParameter(@Nonnull Parameter element, @Nonnull Parameter multiplicity) {
            this.element = element;
            this.multiplicity = multiplicity;
        }

        @Nonnull
        @Override
        public ListParameter bind(@Nonnull Random random) {
            var boundMultiplicity = multiplicity.bind(random);
            Assert.thatUnchecked(boundMultiplicity instanceof PrimitiveParameter, "The multiplicity in ElementMultiplicityListParameter can only be Primitive or Random{Range/Set} Parameter");
            var sqlTypeMultiplicity = ((PrimitiveParameter) boundMultiplicity).getSqlObject(null);
            Assert.thatUnchecked(sqlTypeMultiplicity instanceof Integer || sqlTypeMultiplicity instanceof Long || sqlTypeMultiplicity instanceof Double, "The multiplicity in ElementMultiplicityListParameter only allows Double, Integer or Long");
            Assert.thatUnchecked(((Number) sqlTypeMultiplicity).intValue() > 0, "The multiplicity in ElementMultiplicityListParameter should be > 0");
            return new ListParameter(IntStream.range(0, ((Number) sqlTypeMultiplicity).intValue())
                    .mapToObj(ignored -> element.bind(random))
                    .collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return "unbounded:elementMultiplicityList(element: " + element + ", multiplicity: " + multiplicity + ")";
        }
    }

    /**
     * Returns a list that is a sequence of all numbers between the given lowerBound and upperBound. Applicable for only
     * {@link Integer}, {@link Double}, {@link Float} and {@link Long} parameters.
     */
    public static class ListRangeParameter extends UnboundParameter {
        @Nullable
        private final Parameter lowerBound;
        @Nonnull
        private final Parameter upperBound;

        public ListRangeParameter(@Nonnull Parameter upperbound) {
            this(null, upperbound);
        }

        public ListRangeParameter(@Nullable Parameter lowerBound, @Nonnull Parameter upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Nonnull
        @Override
        public ListParameter bind(@Nonnull Random random) {
            final var lb = RandomRangeParameter.bindAndVerifyNumberParameter(lowerBound, random);
            final var ub = RandomRangeParameter.bindAndVerifyNumberParameter(upperBound, random);
            if (lb instanceof Double || ub instanceof Double) {
                Assert.thatUnchecked(lb.doubleValue() < ub.doubleValue(), "The lower bound parameter should be < the upperbound parameter");
                return new ListParameter(LongStream.range(lb.longValue(), ub.longValue()).asDoubleStream().boxed()
                        .map(PrimitiveParameter::new)
                        .collect(Collectors.toList()));
            } else if (lb instanceof Long || ub instanceof Long) {
                Assert.thatUnchecked(lb.longValue() < ub.longValue(), "The lower bound parameter should be < the upperbound parameter");
                return new ListParameter(LongStream.range(lb.longValue(), ub.longValue()).boxed()
                        .map(PrimitiveParameter::new)
                        .collect(Collectors.toList()));
            } else {
                Assert.thatUnchecked(lb.intValue() < ub.intValue(), "The lower bound parameter should be < the upperbound parameter");
                return new ListParameter(IntStream.range((int) lb, (int) ub).boxed()
                        .map(PrimitiveParameter::new)
                        .collect(Collectors.toList()));
            }
        }

        @Override
        public String toString() {
            return "unbounded:listRange(lowerBound: " + lowerBound + ", upperBound: " + upperBound + ")";
        }
    }
}

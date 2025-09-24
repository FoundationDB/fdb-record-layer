/*
 * BooleanSource.java
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

package com.apple.test;

import org.junit.jupiter.params.provider.ArgumentsSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for parameterized tests that take boolean arguments.
 * One could use {@link org.junit.jupiter.params.provider.ValueSource} for that, but it requires you to explicitly state
 * that you want to run for {@code true} and {@code false}.
 * By default, this will provide {@code false} and {@code true} as the single argument to parameterized tests that have
 * this annotation in that order.
 * If more than one {@code value} is provided, this will produce the same number of boolean arguments, each with the
 * given name.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(BooleanArgumentsProvider.class)
public @interface BooleanSource {
    /**
     * A name to give the boolean values; if unspecified, {@code "true"} and {@code "false"} will be used.
     * If multiple values are given, the cartesian product of {@code true} and {@code false} will be used for each of
     * the names, giving all combinations. There should be one value in this array per boolean parameter to the test.
     * @return the names to be used for the true value for each argument, the false value will be prefixed with "!"
     */
    String[] value() default "";
}

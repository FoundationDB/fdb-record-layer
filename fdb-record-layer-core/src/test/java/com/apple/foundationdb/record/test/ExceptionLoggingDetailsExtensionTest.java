/*
 * ExceptionLoggingDetailsExtensionTest.java
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

package com.apple.foundationdb.record.test;

import com.apple.foundationdb.record.RecordCoreException;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for making sure that the {@link ExceptionLoggingDetailsExtension} works.
 */
@Execution(ExecutionMode.CONCURRENT)
public class ExceptionLoggingDetailsExtensionTest {
    @RegisterExtension
    final ExceptionLoggingDetailsExtension extension = new ExceptionLoggingDetailsExtension();

    @Nonnull
    private static Throwable createCycle() {
        RecordCoreException err1 = new RecordCoreException("err1", "shared_key", "err1", "unique_key_1", 1);
        RecordCoreException err2 = new RecordCoreException("err2", err1).addLogInfo("shared_key", "err2", "unique_key_2", 2);
        RecordCoreException err3 = new RecordCoreException("err3", err2).addLogInfo("shared_key", "err3", "unique_key_3", 3);
        err1.initCause(err3);
        return err1;
    }

    @Nonnull
    static Stream<Arguments> exceptionAndLogInfo() {
        Map<Throwable, Map<String, Object>> errors = Map.of(
                new Throwable("not really thrown"), Collections.emptyMap(),
                new RecordCoreException("not really thrown", "foo", "bar", "baz", 42L), Map.of("foo", "bar", "baz", 42L),
                new RecordCoreException("parent with unique children", new RecordCoreException("child", "inner_key", "inner_value")).addLogInfo("outer_key", "outer_value"), Map.of("outer_key", "outer_value", "inner_key", "inner_value"),
                new RecordCoreException("parent with shared children", new RecordCoreException("child", "shared_key", "inner_value")).addLogInfo("shared_key", "outer_value"), Map.of("shared_key", "outer_value"),
                createCycle(), Map.of("shared_key", "err1", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3),
                createCycle().getCause(), Map.of("shared_key", "err3", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3),
                createCycle().getCause().getCause(), Map.of("shared_key", "err2", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3),
                createCycle().getCause().getCause().getCause(), Map.of("shared_key", "err1", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3)
        );
        return errors.entrySet().stream()
                .flatMap(entry -> Stream.of(
                        Arguments.of(entry.getKey(), entry.getValue()),
                        Arguments.of(new CompletionException(entry.getKey()), entry.getValue()),
                        Arguments.of(new ExecutionException(entry.getKey()), entry.getValue()),
                        Arguments.of(new CompletionException(new ExecutionException(entry.getKey())), entry.getValue()),
                        Arguments.of(new Throwable(entry.getKey()), entry.getValue())
                ));
    }

    @ParameterizedTest
    @MethodSource
    void exceptionAndLogInfo(Throwable err, Map<String, Object> expectedLogInfo) {
        Map<String, Object> collectedInfo = extension.collectLogInfo(err);
        assertEquals(expectedLogInfo, collectedInfo);
    }
}

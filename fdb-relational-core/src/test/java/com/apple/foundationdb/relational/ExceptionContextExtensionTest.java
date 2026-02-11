/*
 * ExceptionContextExtensionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.relational.api.exceptions.ContextualSQLException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
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
 * Tests for making sure that the {@link ExceptionContextExtension} works.
 */
@Execution(ExecutionMode.CONCURRENT)
public class ExceptionContextExtensionTest {
    @RegisterExtension
    final ExceptionContextExtension extension = new ExceptionContextExtension();

    @Nonnull
    private static Throwable createCycle() {
        RelationalException err1 = new RelationalException("err1", ErrorCode.INTERNAL_ERROR)
                .addContext("shared_key", "err1")
                .addContext("unique_key_1", 1);
        RelationalException err2 = new RelationalException("err2", ErrorCode.INTERNAL_ERROR, err1)
                .addContext("shared_key", "err2")
                .addContext("unique_key_2", 2);
        RelationalException err3 = new RelationalException("err3", ErrorCode.INTERNAL_ERROR, err2)
                .addContext("shared_key", "err3")
                .addContext("unique_key_3", 3);
        err1.initCause(err3);
        return err1;
    }

    @Nonnull
    static Stream<Arguments> exceptionAndLogInfo() {
        Map<Throwable, Map<String, Object>> errors = Map.ofEntries(
                Map.entry(new Throwable("not really thrown"), Collections.emptyMap()),
                Map.entry(new RelationalException("not really thrown", ErrorCode.INTERNAL_ERROR)
                        .addContext("foo", "bar")
                        .addContext("baz", 42L),
                        Map.of("foo", "bar", "baz", 42L)),
                Map.entry(new ContextualSQLException("not really thrown", "42000", null, Map.of("ctx_key", "ctx_value")),
                        Map.of("ctx_key", "ctx_value")),
                Map.entry(new RelationalException("parent with unique children", ErrorCode.INTERNAL_ERROR,
                        new RelationalException("child", ErrorCode.INTERNAL_ERROR)
                                .addContext("inner_key", "inner_value"))
                        .addContext("outer_key", "outer_value"),
                        Map.of("outer_key", "outer_value", "inner_key", "inner_value")),
                Map.entry(new RelationalException("parent with shared children", ErrorCode.INTERNAL_ERROR,
                        new RelationalException("child", ErrorCode.INTERNAL_ERROR)
                                .addContext("shared_key", "inner_value"))
                        .addContext("shared_key", "outer_value"),
                        Map.of("shared_key", "outer_value")),
                Map.entry(new RelationalException("parent with ContextualSQLException child", ErrorCode.INTERNAL_ERROR,
                        new ContextualSQLException("sql child", "42000", null, Map.of("sql_key", "sql_value")))
                        .addContext("relational_key", "relational_value"),
                        Map.of("relational_key", "relational_value", "sql_key", "sql_value")),
                Map.entry(createCycle(),
                        Map.of("shared_key", "err1", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3)),
                Map.entry(createCycle().getCause(),
                        Map.of("shared_key", "err3", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3)),
                Map.entry(createCycle().getCause().getCause(),
                        Map.of("shared_key", "err2", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3)),
                Map.entry(createCycle().getCause().getCause().getCause(),
                        Map.of("shared_key", "err1", "unique_key_1", 1, "unique_key_2", 2, "unique_key_3", 3)),
                // RecordCoreException tests
                Map.entry(new RecordCoreException("not really thrown", "foo", "bar", "baz", 42L),
                        Map.of("foo", "bar", "baz", 42L)),
                Map.entry(new RelationalException("parent with RecordCoreException child", ErrorCode.INTERNAL_ERROR,
                        new RecordCoreException("child", "inner_key", "inner_value"))
                        .addContext("outer_key", "outer_value"),
                        Map.of("outer_key", "outer_value", "inner_key", "inner_value")),
                Map.entry(new ContextualSQLException("parent with RecordCoreException child", "42000",
                        new RecordCoreException("child", "record_key", "record_value"), Map.of("sql_key", "sql_value")),
                        Map.of("sql_key", "sql_value", "record_key", "record_value")),
                Map.entry(new RelationalException("parent with shared key", ErrorCode.INTERNAL_ERROR,
                        new RecordCoreException("child", "shared_key", "inner_value"))
                        .addContext("shared_key", "outer_value"),
                        Map.of("shared_key", "outer_value"))
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

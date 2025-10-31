/*
 * DataTypeUtilsTest.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

public class DataTypeUtilsTest {


    static Stream<Arguments> protobufCompliantNameTestArguments() {
        return Stream.of(
                Arguments.of("__", "__"),
                Arguments.of("_", "_"),
                Arguments.of("$", null),
                Arguments.of(".", null),
                Arguments.of("__hello", "__hello"),
                Arguments.of("__$hello", "____1hello"),
                Arguments.of("__$hello", "____1hello"),
                Arguments.of("__.hello", "____2hello"),
                Arguments.of("____hello", "____0hello"),
                Arguments.of("__$h$e$l$l$o", "____1h__1e__1l__1l__1o"),
                Arguments.of("__1", null),
                Arguments.of("$", null),
                Arguments.of("$hello", null),
                Arguments.of(".", null),
                Arguments.of(".hello", null),
                Arguments.of("h__e__l__l__o", "h__0e__0l__0l__0o"),
                Arguments.of("h.e.l.l.o", "h__2e__2l__2l__2o"),
                Arguments.of("h$e$l$l$o", "h__1e__1l__1l__1o"),
                Arguments.of("1hello", null),
                Arguments.of("डेटाबेस", null)
        );
    }

    @ParameterizedTest
    @MethodSource("protobufCompliantNameTestArguments")
    void protobufCompliantNameTest(@Nonnull String userIdentifier, @Nullable String protobufIdentifier) {
        if (protobufIdentifier != null) {
            final var actual = DataTypeUtils.toProtoBufCompliantName(userIdentifier);
            Assertions.assertThat(actual).isEqualTo(protobufIdentifier);
            final var reTranslated = DataTypeUtils.toUserIdentifier(actual);
            Assertions.assertThat(reTranslated).isEqualTo(userIdentifier);
        } else {
            Assertions.assertThatThrownBy(() -> DataTypeUtils.toProtoBufCompliantName(userIdentifier))
                    .isInstanceOf(UncheckedRelationalException.class);
        }
    }

}

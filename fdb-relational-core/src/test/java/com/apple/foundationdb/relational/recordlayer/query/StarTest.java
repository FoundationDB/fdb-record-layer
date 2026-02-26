/*
 * StarTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StarTest {

    @Test
    void withNameThrowsException() {
        final Star star = createSimpleStar();

        assertThatThrownBy(() -> star.withName(Identifier.of("name")))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("attempt to name a star expression");
    }

    @Test
    void asHiddenThrowsException() {
        final Star star = createSimpleStar();

        assertThatThrownBy(star::asHidden)
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("attempt to recreate new star expression");
    }

    @Test
    void asEphemeralThrowsException() {
        final Star star = createSimpleStar();

        assertThatThrownBy(star::asEphemeral)
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("attempt to create an ephemeral expression from a star");
    }

    @Test
    void withUnderlyingThrowsException() {
        final Star star = createSimpleStar();

        assertThatThrownBy(() -> star.withUnderlying(LiteralValue.ofScalar(42L)))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("attempt to replace underlying value of a star expression");
    }

    private Star createSimpleStar() {
        final Expression expr = new Expression(Optional.of(Identifier.of("c1")),
                DataType.LongType.notNullable(), LiteralValue.ofScalar(1L));

        return Star.overIndividualExpressions(
                Optional.of(Identifier.of("t")),
                "unknown",
                Expressions.of(List.of(expr))
        );
    }
}

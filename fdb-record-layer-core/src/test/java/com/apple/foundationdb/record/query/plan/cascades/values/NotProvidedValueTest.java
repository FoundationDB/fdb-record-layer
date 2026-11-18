/*
 * NotProvidedValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NotProvidedValue}.
 */
class NotProvidedValueTest {
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG, false);

    @Test
    void semanticEqualsOnlyItself() {
        final var notProvided = new NotProvidedValue(LONG);
        assertThat(notProvided.semanticEquals(notProvided, AliasMap.emptyMap())).isTrue();
        assertThat(notProvided).isEqualTo(notProvided);

        final var other = new NotProvidedValue(LONG);
        assertThat(notProvided.semanticEquals(other, AliasMap.emptyMap())).isFalse();
        assertThat(notProvided).isNotEqualTo(other);
        assertThat(notProvided.semanticEquals(new NullValue(LONG), AliasMap.emptyMap())).isFalse();
        assertThat(new NullValue(LONG).semanticEquals(notProvided, AliasMap.emptyMap())).isFalse();
    }

    @Test
    void getResultTypeKeepsDeclaredType() {
        assertThat(new NotProvidedValue(LONG).getResultType()).isEqualTo(LONG);
        assertThat(new NotProvidedValue(LONG).getResultType().isNullable()).isFalse();
    }

    @Test
    void evalThrows() {
        assertThatThrownBy(() -> new NotProvidedValue(LONG).eval(null, EvaluationContext.empty()))
                .isInstanceOf(RecordCoreException.class);
    }

    @Test
    void toValueProtoRoundTripsTypeButNotIdentity() {
        final var original = new NotProvidedValue(LONG);
        final var proto = original.toValueProto(PlanSerializationContext.newForCurrentMode());
        final var deserialized = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(deserialized).isInstanceOf(NotProvidedValue.class);
        assertThat(deserialized.getResultType()).isEqualTo(LONG);
        // still unknown after the round trip: equal to nothing but itself
        assertThat(deserialized.semanticEquals(original, AliasMap.emptyMap())).isFalse();
        assertThatThrownBy(() -> deserialized.eval(null, EvaluationContext.empty()))
                .isInstanceOf(RecordCoreException.class);
    }

    @Test
    void toStringRendersPlaceholder() {
        assertThat(new NotProvidedValue(LONG).toString()).isEqualTo("■");
    }
}

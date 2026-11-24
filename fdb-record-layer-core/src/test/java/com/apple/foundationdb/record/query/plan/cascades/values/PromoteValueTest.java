/*
 * PromoteValueTest.java
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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PromoteValue}.
 */
class PromoteValueTest {
    /**
     * Tests that a {@link PromoteValue.ArrayCoercionBiFunction} survives a round trip through its proto representation
     * when its elements trie refers to a record type that is defined by its array types.
     */
    @Test
    void testArrayCoercionBiFunctionSerialization() {
        final MessageHelpers.CoercionBiFunction nested = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, ValueTestHelpers.namedRecordType()),
                new Type.Array(true, ValueTestHelpers.namedRecordType()), null);
        final PromoteValue.ArrayCoercionBiFunction function = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, ValueTestHelpers.namedRecordType()),
                new Type.Array(true, ValueTestHelpers.namedRecordType()),
                new MessageHelpers.CoercionTrieNode(nested, null));

        final var proto = function.toProto(PlanSerializationContext.newForCurrentMode());
        final var deserialized = PromoteValue.ArrayCoercionBiFunction.fromProto(
                PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(deserialized.toProto(PlanSerializationContext.newForCurrentMode())).isEqualTo(proto);
    }
}

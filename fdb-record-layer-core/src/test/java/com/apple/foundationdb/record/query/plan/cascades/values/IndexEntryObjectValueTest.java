/*
 * IndexEntryObjectValueTest.java
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

import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IndexEntryObjectValue}.
 */
class IndexEntryObjectValueTest {

    @Nonnull
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("q");
    @Nonnull
    private static final CorrelationIdentifier OTHER_ALIAS = CorrelationIdentifier.of("p");
    @Nonnull
    private static final Type INT_TYPE = Type.primitiveType(Type.TypeCode.INT);

    /**
     * Tests that two values are equal only if they read the same position of the same tuple through the same alias,
     * and that equal values have the same hash code.
     */
    @Test
    void testIndexEntryObjectValue1() {
        final var keyColumn0 = objectValue(ALIAS, TupleSource.KEY, 0);

        assertThat(keyColumn0).isEqualTo(objectValue(ALIAS, TupleSource.KEY, 0));
        assertThat(keyColumn0).hasSameHashCodeAs(objectValue(ALIAS, TupleSource.KEY, 0));
        assertThat(keyColumn0).isNotEqualTo(objectValue(ALIAS, TupleSource.KEY, 1));
        assertThat(keyColumn0).isNotEqualTo(objectValue(ALIAS, TupleSource.VALUE, 0));
        assertThat(keyColumn0).isNotEqualTo(objectValue(OTHER_ALIAS, TupleSource.KEY, 0));
    }

    @Nonnull
    private static IndexEntryObjectValue objectValue(@Nonnull final CorrelationIdentifier alias,
                                                     @Nonnull final TupleSource source, final int ordinal) {
        return new IndexEntryObjectValue(alias, source, ImmutableIntArray.of(ordinal), INT_TYPE);
    }
}

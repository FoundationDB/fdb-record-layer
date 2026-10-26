/*
 * AvailableFieldsTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopyIfPredicate;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AvailableFields}.
 */
class AvailableFieldsTest {
    private static PlanSerializationContext serializationContext() {
        return new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Test
    void conditionalUponPathPredicateToProto() {
        final AvailableFields.ConditionalUponPathPredicate predicate =
                new AvailableFields.ConditionalUponPathPredicate(ImmutableIntArray.of(3, 1, 4));

        assertThat(predicate.toProto(serializationContext()).getOrdinalPathList())
                .containsExactly(3, 1, 4);
    }

    @Test
    void conditionalUponPathPredicateToProtoWithEmptyPath() {
        final AvailableFields.ConditionalUponPathPredicate predicate =
                new AvailableFields.ConditionalUponPathPredicate(ImmutableIntArray.of());

        assertThat(predicate.toProto(serializationContext()).getOrdinalPathList()).isEmpty();
    }

    @Test
    void conditionalUponPathPredicateToCopyIfPredicateProto() {
        final AvailableFields.ConditionalUponPathPredicate predicate =
                new AvailableFields.ConditionalUponPathPredicate(ImmutableIntArray.of(2, 0));

        final PCopyIfPredicate copyIfPredicate = predicate.toCopyIfPredicateProto(serializationContext());
        assertThat(copyIfPredicate.hasConditionalUponPathPredicate()).isTrue();
        assertThat(copyIfPredicate.getConditionalUponPathPredicate().getOrdinalPathList()).containsExactly(2, 0);
    }
}

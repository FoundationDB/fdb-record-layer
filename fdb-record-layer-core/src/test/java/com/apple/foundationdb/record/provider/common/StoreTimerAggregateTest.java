/*
 * StoreTimerAggregateTest.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link StoreTimer.Aggregate}.
 */
class StoreTimerAggregateTest {
    private enum TestEvent implements StoreTimer.Event {
        FIRST,
        SECOND;

        @Override
        public String title() {
            return name();
        }
    }

    private enum OtherEvent implements StoreTimer.Event {
        OTHER;

        @Override
        public String title() {
            return name();
        }
    }

    private enum TestAggregate implements StoreTimer.Aggregate {
        ONE,
        TWO;

        @Override
        public String title() {
            return name();
        }

        @Nonnull
        @Override
        public Set<? extends StoreTimer.Event> getComponentEvents() {
            return Set.of(TestEvent.FIRST);
        }

        @Nullable
        @Override
        public StoreTimer.Counter compute(@Nonnull final StoreTimer storeTimer) {
            return null;
        }
    }

    @Test
    void defaultValidateReturnsAllEvents() {
        assertThat(TestAggregate.ONE.validate(TestEvent.FIRST, TestEvent.SECOND))
                .containsExactly(TestEvent.FIRST, TestEvent.SECOND);
    }

    @Test
    void defaultValidateAcceptsSingleEvent() {
        assertThat(TestAggregate.ONE.validate(TestEvent.FIRST)).containsExactly(TestEvent.FIRST);
    }

    @Test
    void defaultValidateRejectsNoEvents() {
        assertThatThrownBy(() -> TestAggregate.ONE.validate(new TestEvent[0]))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("At least one event");
    }

    @Test
    void defaultValidateRejectsEventsOfDifferentTypes() {
        assertThatThrownBy(() -> TestAggregate.ONE.validate(new StoreTimer.Event[] {TestEvent.FIRST, OtherEvent.OTHER}))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("same type");
    }

    @Test
    void defaultValidateRejectsAggregatesOfAggregates() {
        assertThatThrownBy(() -> TestAggregate.ONE.validate(new TestAggregate[] {TestAggregate.ONE, TestAggregate.TWO}))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("Aggregates may not be constructed from other aggregates");
    }

    @Test
    void validateAppliesExtraCheckToFirstEventAndEachOther() {
        final List<String> checked = new ArrayList<>();
        final BiConsumer<TestEvent, TestEvent> extraCheck = (first, other) -> checked.add(first.name() + "/" + other.name());
        TestAggregate.ONE.validate(extraCheck, TestEvent.FIRST, TestEvent.SECOND, TestEvent.SECOND);
        assertThat(checked).containsExactly("FIRST/SECOND", "FIRST/SECOND");
    }

    @Test
    void validatePropagatesExtraCheckFailure() {
        final BiConsumer<TestEvent, TestEvent> extraCheck = (first, other) -> {
            throw new RecordCoreArgumentException("Events are incompatible");
        };
        assertThatThrownBy(() -> TestAggregate.ONE.validate(extraCheck, TestEvent.FIRST, TestEvent.SECOND))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("Events are incompatible");
    }
}

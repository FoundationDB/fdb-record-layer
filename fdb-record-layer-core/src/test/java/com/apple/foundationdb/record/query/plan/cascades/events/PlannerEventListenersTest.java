/*
 * PlannerEventListenersTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.events;

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PlannerEventListenersTest {
    @BeforeEach
    void setUp() {
        PlannerEventListeners.clearListeners();
    }

    @AfterAll
    static void tearDown() {
        PlannerEventListeners.clearListeners();
    }

    @Test
    void addSingleListener() {
        final var listener = new DummyEventListener();
        final var plannerEvent = new InitiatePhasePlannerEvent(
                PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN);
        PlannerEventListeners.addListener(DummyEventListener.class, listener);

        PlannerEventListeners.dispatchOnQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT);
        PlannerEventListeners.dispatchEvent(plannerEvent);
        PlannerEventListeners.dispatchOnDone();

        assertThat(PlannerEventListeners.getListener(DummyEventListener.class)).isSameAs(listener);
        assertThat(listener.onQueryCalled).isTrue();
        assertThat(listener.onEventCalled).isTrue();
        assertThat(listener.onDoneCalled).isTrue();
        assertThat(listener.receivedEvent).isSameAs(plannerEvent);
    }

    @Test
    void addMultipleListeners() {
        final var listeners = List.of(new DummyEventListener(), new SecondDummyEventListener());
        final var plannerEvent = new InitiatePhasePlannerEvent(
                PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN);
        listeners.forEach((l) -> PlannerEventListeners.addListener(l.getClass(), l));

        PlannerEventListeners.dispatchOnQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT);
        PlannerEventListeners.dispatchEvent(plannerEvent);
        PlannerEventListeners.dispatchOnDone();

        listeners.forEach(l -> {
            assertThat(PlannerEventListeners.getListener(l.getClass())).isSameAs(l);
            assertThat(l.onQueryCalled).isTrue();
            assertThat(l.onEventCalled).isTrue();
            assertThat(l.onDoneCalled).isTrue();
            assertThat(l.receivedEvent).isSameAs(plannerEvent);
        });
    }

    @Test
    void removeListener() {
        final var listener = new DummyEventListener();
        final var removedListener = new SecondDummyEventListener();
        final var plannerEvent = new InitiatePhasePlannerEvent(
                PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN);

        PlannerEventListeners.addListener(DummyEventListener.class, listener);
        PlannerEventListeners.addListener(SecondDummyEventListener.class, removedListener);
        PlannerEventListeners.removeListener(removedListener.getClass());
        PlannerEventListeners.dispatchOnQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT);
        PlannerEventListeners.dispatchEvent(plannerEvent);
        PlannerEventListeners.dispatchOnDone();

        assertThat(PlannerEventListeners.getListener(DummyEventListener.class)).isSameAs(listener);
        assertThat(listener.onQueryCalled).isTrue();
        assertThat(listener.onEventCalled).isTrue();
        assertThat(listener.onDoneCalled).isTrue();
        assertThat(listener.receivedEvent).isSameAs(plannerEvent);

        assertThat(PlannerEventListeners.getListener(SecondDummyEventListener.class)).isNull();
        assertThat(removedListener.onQueryCalled).isFalse();
        assertThat(removedListener.onEventCalled).isFalse();
        assertThat(removedListener.onDoneCalled).isFalse();
        assertThat(removedListener.receivedEvent).isNull();
    }

    private static class DummyEventListener implements PlannerEventListeners.Listener {
        protected boolean onQueryCalled = false;
        protected boolean onEventCalled = false;
        protected boolean onDoneCalled = false;

        @Nullable protected PlannerEvent receivedEvent = null;

        @Override
        public void onQuery(final String queryAsString, final PlanContext planContext) {
            onQueryCalled = true;
        }

        @Override
        public void onEvent(final PlannerEvent event) {
            onEventCalled = true;
            receivedEvent = event;
        }

        @Override
        public void onDone() {
            onDoneCalled = true;
        }
    }

    private static class SecondDummyEventListener extends DummyEventListener {
    }
}

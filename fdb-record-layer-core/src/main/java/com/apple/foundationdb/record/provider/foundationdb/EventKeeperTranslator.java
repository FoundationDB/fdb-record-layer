/*
 * EventKeeperDelegator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.EventKeeper;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Translator of {@code EventKeeper} events to {@link StoreTimer} events. The {@code EventKeeperTranslator}
 * wraps an underlying {@code StoreTimer} and automatically translates down into the underlying store timer.
 * If the underlying store timer is {@code null}, then all events are silently discarded.
 */
class EventKeeperTranslator implements EventKeeper {
    private static final Map<EventKeeper.Event, StoreTimer.Event> eventKeeperMap = new ConcurrentHashMap<>();

    // The EventKeeper events used to be converted in the FDBStoreTimer and are now done here. For any
    // new EventKeeper events, we automatically compute a name and title for the StoreTimer event, but
    // for pre-existing StoreTimer events that don't fit the way we compute this information, this table
    // is preloaded with the old names.
    static {
        eventKeeperMap.put(EventKeeper.Events.JNI_CALL,
                new Count("JNI_CALLS","jni calls", false));
        eventKeeperMap.put(EventKeeper.Events.RANGE_QUERY_FETCHES,
                new Count("RANGE_FETCHES","range fetches", false));
        eventKeeperMap.put(EventKeeper.Events.RANGE_QUERY_RECORDS_FETCHED,
                new Count("RANGE_KEYVALUES_FETCHED","range key-values ", false));
        eventKeeperMap.put(EventKeeper.Events.RANGE_QUERY_CHUNK_FAILED,
                new Count("CHUNK_READ_FAILURES","read fails", false));
        eventKeeperMap.put(EventKeeper.Events.RANGE_QUERY_FETCH_TIME_NANOS,
                new Event("FETCHES","fetches"));
    }

    @Nullable
    private final StoreTimer underlying;

    public EventKeeperTranslator(@Nullable final StoreTimer underlying) {
        this.underlying = underlying;
    }

    @Override
    public void count(final EventKeeper.Event event, final long amount) {
        if (underlying != null) {
            StoreTimer.Event storeTimerEvent = getEvent(event);
            if (storeTimerEvent instanceof Count) {
                underlying.increment((StoreTimer.Count) storeTimerEvent, (int) amount);
            } else {
                underlying.record(storeTimerEvent, amount);
            }
        }
    }

    @Override
    public void timeNanos(final EventKeeper.Event event, final long nanos) {
        count(event, nanos);
    }

    @Override
    public long getCount(final EventKeeper.Event event) {
        if (underlying == null) {
            return 0L;
        }
        return underlying.getCount(getEvent(event));
    }

    @Override
    public long getTimeNanos(final EventKeeper.Event event) {
        if (underlying == null) {
            return 0L;
        }
        return underlying.getTimeNanos(getEvent(event));
    }

    private static StoreTimer.Event getEvent(EventKeeper.Event event) {
        return eventKeeperMap.computeIfAbsent(event, keeperEvent -> {
            final String name = event.name();
            final String title = event.name().toLowerCase(Locale.ROOT).replace('_', ' ');
            if (event.isTimeEvent()) {
                return new Event(name, title);
            } else {
                return new Count(name, title, false);
            }
        });
    }

    private static class BaseEvent {
        protected final String name;
        protected final String title;
        protected final String logKey;

        public BaseEvent(String name, String title) {
            this.name = name;
            this.title = title;
            this.logKey = name.toLowerCase(Locale.ROOT);
        }

        public String name() {
            return name;
        }

        public String title() {
            return title;
        }

        public String logKey() {
            return logKey;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BaseEvent baseEvent = (BaseEvent)o;
            return Objects.equals(name, baseEvent.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class Event extends BaseEvent implements StoreTimer.Event {
        public Event(String name, String title) {
            super(name, title);
        }
    }

    @SpotBugsSuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
    private static class Count extends BaseEvent implements StoreTimer.Count {
        protected final boolean isSize;

        public Count(String name, String title, boolean isSize) {
            super(name, title);
            this.isSize = isSize;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }
}

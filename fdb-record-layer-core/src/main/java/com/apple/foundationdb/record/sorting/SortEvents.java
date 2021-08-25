/*
 * SortEvents.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

/**
 * {@link StoreTimer} events related to sorting.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.MissingStaticMethodInNonInstantiatableClass")
public class SortEvents {
    private SortEvents() {
    }

    public enum Events implements StoreTimer.Event {
        MEMORY_SORT_STORE_RECORD("memory sort store record"),
        MEMORY_SORT_LOAD_RECORD("memory sort load record"),
        FILE_SORT_OPEN_FILE("file sort open file"),
        FILE_SORT_SAVE_RECORD("file sort save record"),
        FILE_SORT_MERGE_FILES("file sort merge files"),
        FILE_SORT_SKIP_SECTION("file sort skip section"),
        FILE_SORT_SKIP_RECORD("file sort skip record"),
        FILE_SORT_LOAD_RECORD("file sort load record");

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Event.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    public enum Counts implements StoreTimer.Count {
        FILE_SORT_FILE_BYTES("file sort file bytes", true);

        private final String title;
        private final String logKey;
        private final boolean isSize;

        Counts(String title, String logKey, boolean isSize) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Count.super.logKey();
            this.isSize = false;
        }

        Counts(String title, boolean isSize) {
            this(title, null, isSize);
        }

        Counts(String title) {
            this(title, null, false);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }
    
}

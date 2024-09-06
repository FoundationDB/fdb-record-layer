/*
 * ProfileCode.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProfileCode {
    List<Entry> entries = new ArrayList<>();
    long startNanos;

    public ProfileCode() {
        startNanos = System.nanoTime();
    }

    public void add(String name) {
        entries.add(new Entry(name));
    }

    public String message(final String staticMessage) {
        long endNanos = System.nanoTime();
        final KeyValueLogMessage message = KeyValueLogMessage.build(staticMessage);
        message.addKeyAndValue("startNanos", startNanos);
        long lastNanos = startNanos;
        for (final Entry entry : entries) {
            message.addKeyAndValue(entry.name + "Micros", TimeUnit.NANOSECONDS.toMicros(entry.nanos - lastNanos));
            lastNanos = entry.nanos;
        }
        message.addKeyAndValue("totalMicros", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
        message.addKeyAndValue("totalUsec", TimeUnit.NANOSECONDS.toMicros(endNanos - startNanos));
        return message.toString();
    }


    private class Entry {
        String name;
        long nanos;

        public Entry(final String name) {
            this.name = name;
            this.nanos = System.nanoTime();
        }
    }
}

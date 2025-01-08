/*
 * DataSample.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest.datagen;

import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.utils.ReservoirSample;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class DataSample {
    private final Map<String, ReservoirSample<RelationalStruct>> samples = new HashMap<>();

    public void addSample(String tableName, ReservoirSample<RelationalStruct> sample) {
        samples.put(tableName, sample);
    }

    public Iterator<Map<String, RelationalStruct>> getSampleIterator() {
        Map<String, Iterator<RelationalStruct>> iteratorMap = samples.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().sampleIterator()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new Iterator<>() {
            private Map<String, RelationalStruct> nextMap;
            boolean hasNextCalled = false;

            @Override
            public boolean hasNext() {
                if (!hasNextCalled) {
                    hasNextCalled = true;
                    nextMap = iteratorMap.entrySet().stream()
                            .filter(entry -> entry.getValue().hasNext())
                            .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().next()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
                return nextMap != null && !nextMap.isEmpty();
            }

            @Override
            public Map<String, RelationalStruct> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map<String, RelationalStruct> map = nextMap;
                hasNextCalled = false;
                nextMap = null;
                return map;
            }
        };
    }
}

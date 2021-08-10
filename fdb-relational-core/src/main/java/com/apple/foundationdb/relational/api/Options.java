/*
 * Options.java
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

package com.apple.foundationdb.relational.api;

import java.util.HashMap;
import java.util.Map;

public class Options {
    private Map<String,Object> optionsMap = new HashMap<>();

    public static Options create() {
        return new Options();
    }

    @SuppressWarnings("unchecked")
    public <T> T getOption(String name, T defaultValue) {
        return (T) optionsMap.getOrDefault(name,defaultValue);
    }

    public <T> Options withOption(OperationOption<T> option){
        optionsMap.put(option.getOptionName(),option.getValue());
        return this;
    }

    public boolean hasOption(String optionName) {
        return optionsMap.containsKey(optionName);
    }

    public int size() {
        return optionsMap.size();
    }
}

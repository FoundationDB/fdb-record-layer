/*
 * CommandFactory.java
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

package com.apple.foundationdb.relational.cli;

import picocli.CommandLine;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The factory is used by picocli to instantiate Relational commands taking
 * care of their special constructor requirements.
 */
public class CommandFactory implements CommandLine.IFactory {

    private final DbState dbState;

    public CommandFactory(DbState dbState) {
        this.dbState = dbState;
    }

    @Override
    public <K> K create(Class<K> cls) throws Exception {
        List<Constructor<?>> list = Arrays.stream(cls.getDeclaredConstructors()).filter(
                arg -> arg.getParameterCount() == 1 && arg.getParameterTypes()[0] == DbState.class).collect(Collectors.toList());
        if (!list.isEmpty()) {
            return (K) list.get(0).newInstance(dbState);
        }
        return CommandLine.defaultFactory().create(cls);
    }
}

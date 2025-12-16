/*
 * CustomTag.java
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

package com.apple.foundationdb.relational.yamltests.tags;

import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;

public interface CustomTag {

    default void accept(@Nonnull BiConsumer<Tag, Construct> visitor) {
        visitor.accept(getTag(), getConstruct());
    }

    @Nonnull
    Tag getTag();

    @Nonnull
    Construct getConstruct();
}

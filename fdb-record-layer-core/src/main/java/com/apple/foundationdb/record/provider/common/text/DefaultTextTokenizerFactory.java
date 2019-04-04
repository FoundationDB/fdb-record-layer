/*
 * DefaultTextTokenizerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common.text;

import com.apple.foundationdb.annotation.API;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/**
 * Factory class for the {@link DefaultTextTokenizer}. That class is a singleton,
 * so it will return the already existing instance.
 */
@API(API.Status.EXPERIMENTAL)
@AutoService(TextTokenizerFactory.class)
public class DefaultTextTokenizerFactory implements TextTokenizerFactory {
    /**
     * The name of the default text tokenizer. The name of the default tokenizer is
     * "{@value DefaultTextTokenizer#NAME}".
     *
     * @return the name of the default text tokenizer
     */
    @Nonnull
    @Override
    public String getName() {
        return DefaultTextTokenizer.NAME;
    }

    /**
     * The instance of the {@link DefaultTextTokenizer} singleton.
     *
     * @return the instance of the {@link DefaultTextTokenizer}
     */
    @Nonnull
    @Override
    public DefaultTextTokenizer getTokenizer() {
        return DefaultTextTokenizer.instance();
    }
}

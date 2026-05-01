/*
 * BytesTag.java
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

package com.apple.foundationdb.relational.yamltests.tags;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.Hex;
import com.apple.foundationdb.relational.util.Assert;
import com.google.auto.service.AutoService;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Construct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import javax.annotation.Nonnull;

/**
 * A custom YAML tag for byte array values in the {@code x'...'} hex format.
 * <p>
 *     Usage in parameter injection: {@code !! !b x'deadbeef' !!}
 * </p>
 * <p>
 *     Usage in result matching: {@code result: [{!b x'deadbeef'}]}
 * </p>
 */
@AutoService(CustomTag.class)
public class BytesTag implements CustomTag {

    @Nonnull
    private static final Tag tag = new Tag("!b");

    @Nonnull
    private static final Construct CONSTRUCT_INSTANCE = new AbstractConstruct() {
        @Override
        public byte[] construct(final Node node) {
            if (!(node instanceof ScalarNode)) {
                Assert.failUnchecked("The value of the bytes (!b) tag must be a scalar, however '" + node + "' is found!");
            }
            final String value = ((ScalarNode) node).getValue();
            Assert.thatUnchecked(value.startsWith("x'") && value.endsWith("'"),
                    "The value of the bytes (!b) tag must be in x'...' hex format, however '" + value + "' is found!");
            try {
                return Hex.decodeHex(value.substring(2, value.length() - 1));
            } catch (RelationalException e) {
                throw e.toUncheckedWrappedException();
            }
        }
    };

    public BytesTag() {
    }

    @Nonnull
    @Override
    public Tag getTag() {
        return tag;
    }

    @Nonnull
    @Override
    public Construct getConstruct() {
        return CONSTRUCT_INSTANCE;
    }
}

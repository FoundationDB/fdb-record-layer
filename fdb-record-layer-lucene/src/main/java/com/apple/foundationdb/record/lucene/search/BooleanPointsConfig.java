/*
 * BooleanPointsConfig.java
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

package com.apple.foundationdb.record.lucene.search;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import java.text.NumberFormat;

/**
 * A subclass of PointsConfig to allow the Parser the ability to translateCorrelations boolean terms to binary ones.
 */
public class BooleanPointsConfig extends PointsConfig {

    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] TRUE_BYTES = {0x01};
    @SuppressWarnings("squid:S2386")
    @SpotBugsSuppressWarnings("MS_MUTABLE_ARRAY")
    public static final byte[] FALSE_BYTES = {0x00};

    /**
     * Constructs a {@link PointsConfig} object.
     *
     * @param format the {@link NumberFormat} used to parse a {@link String} to
     *               {@link Number} This is actually ignored for Boolean types, but is required by contract.
     * @see PointsConfig#setNumberFormat(NumberFormat)
     */
    public BooleanPointsConfig(NumberFormat format) {
        //we have to use an integer class here to avoid throwing a runtime error at construction time
        super(format, Integer.class);
    }
}

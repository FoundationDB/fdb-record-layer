/*
 * LuceneOptimizedImpacts.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LuceneOptimizedImpacts extends Impacts {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedImpacts.class);

    @Override
    public int numLevels() {
        if (LOG.isInfoEnabled()) {
            LOG.info("numLevels");
        }
        return 0;
    }

    @Override
    public int getDocIdUpTo(final int level) {
        if (LOG.isInfoEnabled()) {
            LOG.info("getDocIdUpTo");
        }
        return 0;
    }

    @Override
    public List<Impact> getImpacts(final int level) {
        if (LOG.isInfoEnabled()) {
            LOG.info("getImpacts");
        }
        return new ArrayList<>();
    }
}

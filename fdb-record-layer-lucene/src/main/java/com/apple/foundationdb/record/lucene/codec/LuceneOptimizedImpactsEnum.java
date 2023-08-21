/*
 * LuceneOptimizedImpactsEnum.java
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

import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LuceneOptimizedImpactsEnum extends ImpactsEnum {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedImpactsEnum.class);
    @Override
    public void advanceShallow(final int target) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("advanceShallow");
        }
    }

    @Override
    public Impacts getImpacts() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getImpacts");
        }
        return new LuceneOptimizedImpacts();
    }

    @Override
    public int freq() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("freq");
        }
        return 0;
    }

    @Override
    public int nextPosition() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("nextPosition");
        }
        return 0;
    }

    @Override
    public int startOffset() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("startOffset");
        }
        return 0;
    }

    @Override
    public int endOffset() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("endOffset");
        }
        return 0;
    }

    @Override
    public BytesRef getPayload() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("getPayload");
        }
        return null;
    }

    @Override
    public int docID() {
        if (LOG.isInfoEnabled()) {
            LOG.info("docID");
        }
        return 0;
    }

    @Override
    public int nextDoc() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("nextDoc");
        }
        return 0;
    }

    @Override
    public int advance(final int target) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("advance");
        }
        return 0;
    }

    @Override
    public long cost() {
        if (LOG.isInfoEnabled()) {
            LOG.info("cost");
        }
        return 0;
    }
}

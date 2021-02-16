/*
 * FDBIndexInputTest.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Test to FDBIndexOutput functionality.
 *
 */
@Tag(Tags.RequiresFDB)
public class FDBIndexOutputTest extends FDBDirectoryBaseTest {
    private static final String FILE_NAME = "y";

    @Test
    public void testWriteBytes() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        byte[] data = new byte[randomInt(256)];
        random.nextBytes(data);
        output.writeBytes(data, data.length);
        output.close();
        assertEquals(data.length, directory.getFDBLuceneFileReference(FILE_NAME).get().getSize());
    }
    
    @Test
    public void testWriteByte() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeByte((byte) 0);
        output.close();
        assertEquals(1, directory.getFDBLuceneFileReference(FILE_NAME).get().getSize());
    }

}

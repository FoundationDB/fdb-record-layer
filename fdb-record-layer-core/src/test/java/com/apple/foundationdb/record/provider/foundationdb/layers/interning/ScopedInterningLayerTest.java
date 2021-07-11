/*
 * ScopedInterningLayerTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolverTest;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedDirectoryLayer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.TestingResolverFactory;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link ScopedInterningLayer}.
 */
@Tag(Tags.WipesFDB)
@Tag(Tags.RequiresFDB)
public class ScopedInterningLayerTest extends LocatableResolverTest {
    public ScopedInterningLayerTest() {
        super(TestingResolverFactory.ResolverType.SCOPED_INTERNING_LAYER);
    }

    @BeforeEach
    public void retireOldDefault() {
        ScopedDirectoryLayer.global(database).retireLayer().join();
    }

    @AfterEach
    public void teardown() {
        // tests that modify migration state of global directory layer need to wipe FDB to prevent pollution of other suites
        resolverFactory.wipeFDB();
    }


    @Test
    public void testUpdateMetadataKeyDoesNotExist() {
        byte[] metadata = Tuple.from("old").pack();
        try {
            globalScope.updateMetadataAndVersion("some-key-that-does-not-exist", metadata).join();
            fail("should throw NoSuchElementException");
        } catch (CompletionException ex) {
            assertThat(ex.getCause(), is(instanceOf(NoSuchElementException.class)));
            assertThat(ex.getCause(), hasMessageContaining("updateMetadata must reference key that already exists"));
        }
    }

}

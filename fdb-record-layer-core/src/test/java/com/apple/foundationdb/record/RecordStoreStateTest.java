/*
 * RecordStoreStateTest.java
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

package com.apple.foundationdb.record;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RecordStoreState}.
 */
public class RecordStoreStateTest {

    @Nonnull
    private static RecordStoreState stateOf(@Nonnull Object... values) {
        if (values.length % 2 != 0) {
            throw new RecordCoreArgumentException("odd number of values given to create record store state");
        }
        Map<String, IndexState> indexStateMap = Maps.newHashMapWithExpectedSize(values.length / 2);
        for (int i = 0; i < values.length; i += 2) {
            String indexName = (String)values[i];
            IndexState indexState = (IndexState)values[i + 1];
            indexStateMap.put(indexName, indexState);
        }
        return new RecordStoreState(null, indexStateMap);
    }

    @Nonnull
    private Set<String> indexNamesWithState(@Nonnull RecordStoreState storeState, @Nonnull IndexState indexState) {
        return storeState.getIndexStates().entrySet().stream()
                .filter(entry -> entry.getValue().equals(indexState))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Test
    public void emptyState() {
        RecordStoreState storeState = stateOf();
        assertFalse(storeState.isDisabled("asdf"));
        assertFalse(storeState.isWriteOnly("asdf"));
        assertTrue(storeState.isReadable("asdf"));
        assertTrue(storeState.allIndexesReadable());
        assertEquals(Collections.emptyMap(), storeState.getIndexStates());
    }

    @Test
    public void oneWriteOnly() {
        RecordStoreState storeState = stateOf("asdf", IndexState.WRITE_ONLY);
        assertFalse(storeState.isDisabled("asdf"));
        assertTrue(storeState.isWriteOnly("asdf"));
        assertFalse(storeState.isReadable("asdf"));
        assertFalse(storeState.allIndexesReadable());
        assertEquals(Collections.singleton("asdf"), indexNamesWithState(storeState, IndexState.WRITE_ONLY));
        assertEquals(Collections.emptySet(), indexNamesWithState(storeState, IndexState.DISABLED));
    }

    @Test
    public void oneDisabled() {
        RecordStoreState storeState = stateOf("asdf", IndexState.DISABLED);
        assertTrue(storeState.isDisabled("asdf"));
        assertFalse(storeState.isWriteOnly("asdf"));
        assertFalse(storeState.isReadable("asdf"));
        assertFalse(storeState.allIndexesReadable());
        assertEquals(Collections.emptySet(), indexNamesWithState(storeState, IndexState.WRITE_ONLY));
        assertEquals(Collections.singleton("asdf"), indexNamesWithState(storeState, IndexState.DISABLED));
    }

    @Test
    public void oneReadable() {
        RecordStoreState storeState = stateOf("asdf", IndexState.READABLE);
        assertFalse(storeState.isDisabled("asdf"));
        assertFalse(storeState.isWriteOnly("asdf"));
        assertTrue(storeState.isReadable("asdf"));
        assertTrue(storeState.allIndexesReadable());
        assertEquals(Collections.emptySet(), indexNamesWithState(storeState, IndexState.WRITE_ONLY));
        assertEquals(Collections.emptySet(), indexNamesWithState(storeState, IndexState.DISABLED));
    }

    @Test
    public void compatibleWith() {
        final RecordStoreState empty = stateOf();
        final RecordStoreState oneWriteOnly = stateOf("one", IndexState.WRITE_ONLY);
        final RecordStoreState oneDisabled = stateOf("one", IndexState.DISABLED);
        final RecordStoreState hasBoth = stateOf("one", IndexState.WRITE_ONLY, "two", IndexState.DISABLED);

        assertTrue(empty.compatibleWith(oneWriteOnly));
        assertFalse(oneWriteOnly.compatibleWith(empty));

        assertTrue(empty.compatibleWith(oneDisabled));
        assertFalse(oneDisabled.compatibleWith(empty));

        assertTrue(empty.compatibleWith(hasBoth));
        assertFalse(hasBoth.compatibleWith(empty));

        assertTrue(oneDisabled.compatibleWith(oneWriteOnly));
        assertTrue(oneWriteOnly.compatibleWith(oneDisabled));

        assertTrue(oneWriteOnly.compatibleWith(hasBoth));
        assertFalse(hasBoth.compatibleWith(oneWriteOnly));

        assertTrue(oneDisabled.compatibleWith(hasBoth));
        assertFalse(hasBoth.compatibleWith(oneDisabled));
    }

    @Test
    public void testWithWriteOnlyIndexes() {
        final String implicitlyReadable = "Implicitly readable";
        final String explicitlyReadable = "Explicitly readable";
        final String disabled = "Disabled";
        final String writeOnly = "Write only";
        final String newWriteOnly = "New write only";
        RecordStoreState state = stateOf(
                explicitlyReadable, IndexState.READABLE,
                disabled, IndexState.DISABLED,
                writeOnly, IndexState.WRITE_ONLY);
        assertEquals(IndexState.READABLE, state.getState(implicitlyReadable));
        assertEquals(IndexState.READABLE, state.getState(explicitlyReadable));
        assertEquals(IndexState.DISABLED, state.getState(disabled));
        assertEquals(IndexState.WRITE_ONLY, state.getState(writeOnly));
        RecordStoreState newState = state.withWriteOnlyIndexes(
                Arrays.asList(implicitlyReadable, explicitlyReadable, disabled, writeOnly), null);
        assertEquals(IndexState.WRITE_ONLY, newState.getState(implicitlyReadable));
        assertEquals(IndexState.WRITE_ONLY, newState.getState(explicitlyReadable));
        assertEquals(IndexState.DISABLED, newState.getState(disabled));
        assertEquals(IndexState.WRITE_ONLY, newState.getState(writeOnly));
        assertEquals(IndexState.READABLE, newState.getState("other implicitly readable"));
        // no changes to original
        assertEquals(IndexState.READABLE, state.getState(implicitlyReadable));
        assertEquals(IndexState.READABLE, state.getState(explicitlyReadable));
        assertEquals(IndexState.DISABLED, state.getState(disabled));
        assertEquals(IndexState.WRITE_ONLY, state.getState(writeOnly));

        RecordStoreState newState2 = state.withWriteOnlyIndexes(Collections.singletonList(newWriteOnly), null);
        assertEquals(IndexState.READABLE, newState2.getState(implicitlyReadable));
        assertEquals(IndexState.READABLE, newState2.getState(explicitlyReadable));
        assertEquals(IndexState.DISABLED, newState2.getState(disabled));
        assertEquals(IndexState.WRITE_ONLY, newState2.getState(writeOnly));
        assertEquals(IndexState.WRITE_ONLY, newState2.getState(newWriteOnly));

    }

    @Test
    public void testReadWriteExclusion() {
        MutableRecordStoreState state = stateOf().toMutable();
        state.beginRead();
        state.beginRead();
        state.endRead();
        state.endRead();
        state.beginWrite();
        state.beginWrite();
        state.endWrite();
        Exception ex = assertThrows(RecordCoreException.class, state::beginRead);
        assertThat(ex.getMessage(), containsString("record store state is being modified"));
        state.endWrite();
        state.beginRead();
        ex = assertThrows(RecordCoreException.class, state::beginWrite);
        assertThat(ex.getMessage(), containsString("record store state is being used for queries"));
    }
}

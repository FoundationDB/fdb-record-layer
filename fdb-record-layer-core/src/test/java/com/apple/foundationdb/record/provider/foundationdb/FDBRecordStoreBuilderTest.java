package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FDBRecordStoreBuilderTest extends FDBRecordStoreTestBase {


    RecordMetaDataBuilder records1Builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
    RecordMetaDataBuilder records2Builder = RecordMetaData.newBuilder().setRecords(TestRecords2Proto.getDescriptor());

    @Test
    void testReuseStoreOnSameTransaction() {
        final Function<FDBRecordContext, FDBRecordStore> createStore = context -> FDBRecordStore.newBuilder()
                .setKeySpacePath(getPath("X"))
                .setContext(context)
                .setMetaDataProvider(records1Builder)
                .build();
        FDBRecordStore store1;
        try (FDBRecordContext context = openContext()) {
            store1 = createStore.apply(context);
            final FDBRecordStore store2 = createStore.apply(context);
            assertSame(store1, store2);
            // make sure it's not cached across contexts
            try (FDBRecordContext context2 = openContext()) {
                final FDBRecordStore store3 = createStore.apply(context2);
                assertNotSame(store1, store3);
            }
        }
    }

    @Test
    void testFailIfDifferentStoreExistsOnTransaction() {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(getPath("X"))
                    .setContext(context)
                    .setMetaDataProvider(records1Builder)
                    .build();
            // a store with a different metadata in the same subspace
            assertThrows(RecordCoreArgumentException.class, () -> FDBRecordStore.newBuilder()
                    .setKeySpacePath(getPath("X"))
                    .setContext(context)
                    .setMetaDataProvider(records2Builder)
                    .build());
            // different metadata, but also different subspace
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(getPath("Y"))
                    .setContext(context)
                    .setMetaDataProvider(records2Builder)
                    .build();
        }
    }

    @Test
    void testMultipleStoresForDifferentKeyspacesOnSameTransaction() {
        final BiFunction<FDBRecordContext, String, FDBRecordStore> createStore =
                (context, id) -> FDBRecordStore.newBuilder()
                .setKeySpacePath(getPath(id))
                .setContext(context)
                .setMetaDataProvider(records1Builder)
                .build();
        FDBRecordStore store1;
        try (FDBRecordContext context = openContext()) {
            store1 = createStore.apply(context, "X");
            final FDBRecordStore store2 = createStore.apply(context, "Y");
            assertNotSame(store1, store2);
        }
    }


    private KeySpacePath getPath(final String id) {
        return TestKeySpace.keySpace.path("record-test")
                .add("unit")
                .add("multiRecordStore")
                .add("storePath", id);
    }
}

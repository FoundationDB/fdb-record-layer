package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class TestFDBDirectory extends FDBDirectory {
    public TestFDBDirectory() {
        super(new Subspace(Tuple.from("record-test", "unit", "lucene")),
                FDBDatabaseFactory.instance().getDatabase().openContext());
    }
}

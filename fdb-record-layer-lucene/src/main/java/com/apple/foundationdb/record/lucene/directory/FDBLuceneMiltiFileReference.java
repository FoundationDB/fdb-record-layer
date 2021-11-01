/*
 * FDBLuceneMiltiFileReference.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FDBLuceneMiltiFileReference {
    private Map<String, FDBLuceneFileReference> files;

    public FDBLuceneMiltiFileReference() {
        files = new HashMap<>();
    }

    public FDBLuceneMiltiFileReference(@Nonnull Tuple tuple) {
        files = new HashMap<>();
        for (int i = 0; i<tuple.size(); i++) {
            Tuple file = tuple.getNestedTuple(i);
            String fileName = file.getString(0);
            FDBLuceneFileReference fileReference = new FDBLuceneFileReference(file.getNestedTuple(1));
            files.put(fileName, fileReference);
        }
    }

    public FDBLuceneFileReference getReference(String reference) {
        return files.get(reference);
    }

    public Set<Map.Entry<String, FDBLuceneFileReference>> listReferences() {
        return files.entrySet();
    }

    public void putNewReference(String reference, FDBLuceneFileReference fileReference) {
        files.put(reference, fileReference);
    }

    public void renameReference(String source, String dest) {
        FDBLuceneFileReference reference = files.remove(source);
        if (reference == null) {
            throw new RecordCoreArgumentException("Invalid source name in rename function for source")
                    .addLogInfo(LogMessageKeys.SOURCE_FILE,source)
                    .addLogInfo(LogMessageKeys.INDEX_TYPE, IndexTypes.LUCENE);
        }
        files.put(dest, reference);
    }

    public Tuple getTuple() {
        Tuple reference = new Tuple();
        for (Map.Entry<String, FDBLuceneFileReference> file : files.entrySet()) {
            reference = reference.add(Tuple.from(file.getKey(), file.getValue().getTuple()));
        }
        return reference;
    }


}

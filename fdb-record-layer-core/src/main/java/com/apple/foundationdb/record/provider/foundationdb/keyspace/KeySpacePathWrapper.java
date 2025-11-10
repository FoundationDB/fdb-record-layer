/*
 * KeySpacePathWrapper.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.ValueRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Helper class to make it easy to create <code>KeySpacePath</code> wrapper implementations for use in
 * {@link KeySpaceDirectory#KeySpaceDirectory(String, KeySpaceDirectory.KeyType, Object, java.util.function.Function)} (String, KeySpaceDirectory.KeyType, Object, Function)}.
 * Wrappers are most useful for making it easy for clients of the KeySpacePath API to work with path
 * elements using concrete method calls rather than logical directory names.  For example:
 * <pre>
 *     public enum PhoneType {
 *         WORK,
 *         HOME,
 *         MOBILE
 *     }
 *
 *     public class EmployeePath() extends KeySpacePathWrapper {
 *         public EmployeePath(KeySpacePath path) {
 *             super(path);
 *         }
 *         public PhonePath() phone(PhoneType phoneType) {
 *             return (PhonePath) inner.add("phone", phoneType.toString());
 *         }
 *         public CorporateRoot parent() {
 *             return (CorporateRoot) inner.getParent();
 *         }
 *     }
 *
 *     public class PhonePath() extends KeySpacePathWrapper {
 *         public PhonePath(KeySpacePath path) {
 *             super(path);
 *         }
 *         public parent() {
 *             return (EmployeePath) inner.getParent();
 *         }
 *     }
 *
 *     public class CorporateRoot() {
 *         private KeySpace root =
 *             new KeySpace(
 *                 new KeySpaceDirectory("employee", KeyType.UUID, EmployeePath::new)
 *                     .addSubdirectory(new KeySpaceDirectory("phone", KeyType.STRING, PhonePath::new)));
 *
 *         public EmployeePath employee(UUID uuid) {
 *             return (EmployeePath) root.path("employee", uuid);
 *         }
 *     }
 * </pre>
 *
 * Here, we have created two wrappers, one around the <code>employee</code> directory and another around its
 * <code>phone</code> subdirectory.  Each of these wrappers will automatically wrap the actual path entry that is
 * returned when these directories are accessed and each decorate the path entry with methods that allow you to
 * explicitly refer to directories by method name and to perform type validate at compile time.  For example:
 * <pre>
 *     CorporateRoot root = new CorporateRoot();
 *     PhonePath phonePath = root.employee(myUuid).phone(PhoneType.WORK);
 * </pre>
 */
@API(API.Status.UNSTABLE)
public class KeySpacePathWrapper implements KeySpacePath {
    protected KeySpacePath inner;

    public KeySpacePathWrapper(KeySpacePath inner) {
        this.inner = inner;
    }

    @Override
    @Nonnull
    public KeySpacePath add(@Nonnull String dirName) {
        return inner.add(dirName);
    }

    @Override
    @Nonnull
    public KeySpacePath add(@Nonnull String dirName, @Nullable Object value) {
        return inner.add(dirName, value);
    }

    @Override
    @Nullable
    public KeySpacePath getParent() {
        return inner.getParent();
    }

    @Override
    @Nonnull
    public String getDirectoryName() {
        return inner.getDirectoryName();
    }

    @Override
    @Nonnull
    public KeySpaceDirectory getDirectory() {
        return inner.getDirectory();
    }

    @Override
    public Object getValue() {
        return inner.getValue();
    }

    @Override
    @Nonnull
    public CompletableFuture<PathValue> resolveAsync(@Nonnull FDBRecordContext context) {
        return inner.resolveAsync(context);
    }

    @Override
    @Nonnull
    public CompletableFuture<Tuple> toTupleAsync(@Nonnull FDBRecordContext context) {
        return inner.toTupleAsync(context);
    }

    @Override
    @Nonnull
    public List<KeySpacePath> flatten() {
        return inner.flatten();
    }

    @Override
    @Nonnull
    public CompletableFuture<Boolean> hasDataAsync(@Nonnull FDBRecordContext context) {
        return inner.hasDataAsync(context);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> deleteAllDataAsync(@Nonnull FDBRecordContext context) {
        return inner.deleteAllDataAsync(context);
    }

    @Nonnull
    @Override
    public RecordCursor<ResolvedKeySpacePath> listSubdirectoryAsync(@Nonnull FDBRecordContext context,
                                                                    @Nonnull String subdirName,
                                                                    @Nullable ValueRange<?> range,
                                                                    @Nullable byte[] continuation,
                                                                    @Nonnull ScanProperties scanProperties) {
        return inner.listSubdirectoryAsync(context, subdirName, range, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public CompletableFuture<ResolvedKeySpacePath> toResolvedPathAsync(@Nonnull FDBRecordContext context) {
        return inner.toResolvedPathAsync(context);
    }

    @Override
    public boolean equals(Object obj) {
        return inner.equals(obj);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }

    @Override
    public String toString() {
        return inner.toString();
    }

    @Override
    public String toString(@Nonnull Tuple t) {
        return inner.toString(t);
    }

    @Nonnull
    @Override
    public RecordCursor<DataInKeySpacePath> exportAllData(@Nonnull FDBRecordContext context,
                                                          @Nullable byte[] continuation,
                                                          @Nonnull ScanProperties scanProperties) {
        return inner.exportAllData(context, continuation, scanProperties);
    }
}

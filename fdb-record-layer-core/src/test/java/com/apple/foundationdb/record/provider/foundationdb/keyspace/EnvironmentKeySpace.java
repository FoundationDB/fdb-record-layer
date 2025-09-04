/*
 * EnvironmentKeySpace.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;

/**
 * This provides an example of a way in which you can define a KeySpace in a relatively clean and type-safe
 * manner. It defines a keyspace that looks like:
 * <pre>
 *    [environment]           - A string the identifies the logical environment (like prod, test, qa, etc.).
 *      |                       This string is converted by the directory layer as a small integer value.
 *      +- userid             - An integer ID for each user in the system
 *         |
 *         +- [application]   - Tne name of an application the user runs (again, converted by the directory
 *            |                 layer into a small integer value)
 *            +- data=1       - Constant value of "1", which is the location of a {@link FDBRecordStore}
 *            |                 in which application data is to be stored
 *            +- metadata=2   - Constant value of "2", which is the Location of another <code>FDBRecordStore</code>
 *                              in which application metadata or configuration information can live.
 * </pre>
 * The main point of this class is to demonstrate how you can use the KeySpacePath wrapping facility to provide
 * implementations of the path elements that are meaningful to your application environment and type safe.
 */
class EnvironmentKeySpace {
    private final KeySpace root;
    private final String rootName;

    static final String USER_KEY = "userid";
    static final String APPLICATION_KEY = "application";
    static final String DATA_KEY = "data";
    static final long DATA_VALUE = 1L;
    static final String METADATA_KEY = "metadata";
    static final long METADATA_VALUE = 2L;

    /**
     * The <code>EnvironmentKeySpace</code> scopes all of the data it stores underneath of a <code>rootName</code>,
     * for example, you could define an instance for <code>prod</code>, <code>test</code>, <code>qa</code>, etc.
     *
     * @param rootName The root name underwhich all data is stored.
     */
    public EnvironmentKeySpace(String rootName) {
        this.rootName = rootName;
        root = new KeySpace(
                new DirectoryLayerDirectory(rootName, rootName, EnvironmentRoot::new)
                        .addSubdirectory(new KeySpaceDirectory(USER_KEY, KeySpaceDirectory.KeyType.LONG, UserPath::new)
                                .addSubdirectory(new DirectoryLayerDirectory(APPLICATION_KEY, ApplicationPath::new)
                                        .addSubdirectory(new KeySpaceDirectory(DATA_KEY, KeySpaceDirectory.KeyType.LONG, DATA_VALUE, DataPath::new))
                                        .addSubdirectory(new KeySpaceDirectory(METADATA_KEY, KeySpaceDirectory.KeyType.LONG, METADATA_VALUE, MetadataPath::new)))));
    }

    public String getRootName() {
        return rootName;
    }

    /**
     * Returns an implementation of a <code>KeySpacePath</code> that represents the start of the environment.
     */
    public EnvironmentRoot root() {
        return (EnvironmentRoot)root.path(rootName);
    }

    /**
     * Given a tuple that represents an FDB key that came from this KeySpace, returns the leaf-most path
     * element in which the tuple resides.
     */
    public ResolvedKeySpacePath fromKey(FDBRecordContext context, Tuple tuple) {
        return root.resolveFromKey(context, tuple);
    }

    /**
     * A <code>KeySpacePath</code> that represents the logical root of the environment.
     */
    static class EnvironmentRoot extends KeySpacePathWrapper {
        public EnvironmentRoot(KeySpacePath path) {
            super(path);
        }

        public KeySpacePath parent() {
            return null;
        }

        public UserPath userid(long userid) {
            return (UserPath) inner.add(USER_KEY, userid);
        }
    }

    static class UserPath extends KeySpacePathWrapper {
        public UserPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath application(String applicationName) {
            return (ApplicationPath) inner.add(APPLICATION_KEY, applicationName);
        }

        public EnvironmentRoot parent() {
            return (EnvironmentRoot) inner.getParent();
        }
    }

    static class ApplicationPath extends KeySpacePathWrapper {
        public ApplicationPath(KeySpacePath path) {
            super(path);
        }

        public DataPath dataStore() {
            return (DataPath) inner.add(DATA_KEY);
        }

        public MetadataPath metadataStore() {
            return (MetadataPath) inner.add(METADATA_KEY);
        }

        public UserPath parent() {
            return (UserPath) inner.getParent();
        }
    }

    static class DataPath extends KeySpacePathWrapper {
        public DataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }

    static class MetadataPath extends KeySpacePathWrapper {
        public MetadataPath(KeySpacePath path) {
            super(path);
        }

        public ApplicationPath parent() {
            return (ApplicationPath) inner.getParent();
        }
    }
}

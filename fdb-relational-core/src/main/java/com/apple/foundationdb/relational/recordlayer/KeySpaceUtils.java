/*
 * KeySpaceUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

public final class KeySpaceUtils {

    @Nonnull
    public static URI pathToUri(@Nonnull KeySpacePath dbPath) {
        return URI.create("/" + toPathString(dbPath));
    }

    public static String toPathString(KeySpacePath path) {
        String uriPath = "";
        while (path != null) {
            if (path.getDirectory().getKeyType() == KeySpaceDirectory.KeyType.NULL) {
                uriPath = "/" + uriPath;
            } else {
                if (uriPath.length() > 0) {
                    uriPath = path.getValue().toString() + "/" + uriPath;
                } else {
                    uriPath = path.getValue().toString();
                }
            }
            path = path.getParent();
        }
        return uriPath;
    }

    @VisibleForTesting
    @Nonnull
    public static KeySpacePath toKeySpacePath(@Nonnull URI url, KeySpace keySpace) throws RelationalException {
        String path = getPath(url);
        if (path.length() < 1) {
            throw new RelationalException("<" + url + "> is an invalid database path", ErrorCode.INVALID_PATH);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        String[] pathElems = path.split("/");
        //TODO(bfines): this is super inefficient, we need to replace it with something more coherent
        if (path.endsWith("/")) {
            pathElems = Arrays.copyOf(pathElems, pathElems.length + 1);
            pathElems[pathElems.length - 1] = "";
        }
        if (pathElems.length == 2 && RelationalKeyspaceProvider.SYS.equals(pathElems[1])) {
            return keySpace.path(RelationalKeyspaceProvider.SYS).add(RelationalKeyspaceProvider.SYS);
        }
        KeySpaceDirectory directory = keySpace.getRoot();
        KeySpacePath thePath = null;
        for (KeySpaceDirectory sub : directory.getSubdirectories()) {
            thePath = uriToPathRecursive(keySpace, sub, keySpace.path(sub.getName()), pathElems, 1);
            if (thePath != null) {
                break;
            }
        }

        if (thePath == null) {
            throw new RelationalException("<" + url + "> is an invalid database path", ErrorCode.INVALID_PATH);
        }
        return thePath;
    }

    public static String getPath(@Nonnull URI url) {
        String authority = url.getAuthority();
        return authority != null && authority.length() > 0 ? "//" + authority + url.getPath() : url.getPath();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    @Nullable
    private static KeySpacePath uriToPathRecursive(@Nonnull KeySpace keySpace,
                                    @Nonnull KeySpaceDirectory directory,
                                    KeySpacePath parentPath,
                                    @Nonnull String[] pathElems,
                                    int position) throws OperationUnsupportedException {
        if (position >= pathElems.length) {
            return parentPath;
        }
        String pathElem = pathElems[position];
        String pathName = directory.getName();
        Object dirVal = directory.getValue();
        Object pathValue = null;
        switch (directory.getKeyType()) {
            case NULL:
                // empty string in URI represents NULL value, and non-empty value is invalid for directory with NULL KeyType
                if (!pathElem.isEmpty()) {
                    return null;
                }
                break;
            case STRING:
                pathValue = pathElem;
                // empty string in URI represents NULL value, and empty value is invalid for directory with String KeyType
                if (pathElem.isEmpty()) {
                    return null;
                } else if (Objects.equals(dirVal, KeySpaceDirectory.ANY_VALUE)) {
                    break;
                } else if (!Objects.equals(dirVal, pathElem)) {
                    return null;
                }
                break;
            case LONG:
                if (directory instanceof DirectoryLayerDirectory) {
                    pathValue = pathElem;
                } else {
                    try {
                        long l = Long.parseLong(pathElem);
                        pathValue = l;
                        if (Objects.equals(dirVal, KeySpaceDirectory.ANY_VALUE)) {
                            break;
                        } else if (!Objects.equals(dirVal, l)) {
                            return null;
                        }
                    } catch (NumberFormatException nfe) {
                        //the field isn't a long, so can't match this directory
                        return null;
                    }
                }
                if (!Objects.equals(dirVal, KeySpaceDirectory.ANY_VALUE) && !Objects.equals(dirVal, pathValue)) {
                    return null;
                }
                break;
            default:
                throw new OperationUnsupportedException("Key Space paths only supported for NULL, LONG and STRING");
        }

        try {
            if (directory.getParent() == keySpace.getRoot()) {
                parentPath = keySpace.path(pathName, pathValue);
            } else {
                parentPath = parentPath.add(pathName, pathValue);
            }
        } catch (NoSuchDirectoryException nsde) {
            //safety valve--shouldn't really ever be used, but if it happens we know it's not a valid path
            return null;
        }

        if (directory.isLeaf()) {
            return parentPath; //we found the path!
        }

        for (KeySpaceDirectory dir : directory.getSubdirectories()) {
            KeySpacePath childPath = uriToPathRecursive(keySpace, dir, parentPath, pathElems, position + 1);
            if (childPath != null) {
                return childPath;
            }
        }
        //no valid path
        return null;
    }

    private KeySpaceUtils() {
    }

}

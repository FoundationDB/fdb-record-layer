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

import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    @Nonnull
    public static KeySpacePath uriToPath(@Nonnull URI url, @Nonnull KeySpace keySpace) throws RelationalException {
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

    public static KeySpacePath getSchemaPath(@Nonnull URI schemaUrl, @Nonnull KeySpace keySpace) throws RelationalException {
        String schemaPath = getPath(schemaUrl);
        int indexOfLastSlash = schemaPath.lastIndexOf("/");
        if (indexOfLastSlash < 1) {
            throw new RelationalException("Invalid schemaUrl: <" + schemaUrl + ">", ErrorCode.INVALID_PATH);
        }
        String schemaId = schemaPath.substring(indexOfLastSlash + 1);
        if (schemaId.isEmpty()) {
            throw new RelationalException("Invalid schemaUrl with empty schema: <" + schemaUrl + ">", ErrorCode.INVALID_PATH);
        }
        URI dbUrl = URI.create(schemaPath.substring(0, indexOfLastSlash));
        KeySpacePath dbPath = uriToPath(dbUrl, keySpace);
        extendKeySpaceForSchema(keySpace, dbPath, schemaId);
        return dbPath.add(schemaId);
    }

    public static String getPath(@Nonnull URI url) {
        String authority = url.getAuthority();
        return authority != null && authority.length() > 0 ? "//" + authority + url.getPath() : url.getPath();
    }

    /**
     * Add the directory in the keySpace with the schemaId as the name.
     *
     * @param keySpace the KeySpace to add directory onto
     * @param dbPath   the KeySpacePath used to find the database directory
     * @param schemaId the identifier for the schema to add
     * @return A KeySpace which includes an extended directory structure for the specified schema.
     */
    public static KeySpace extendKeySpaceForSchema(@Nonnull KeySpace keySpace, @Nonnull KeySpacePath dbPath, @Nonnull String schemaId) {
        KeySpaceDirectory current = keySpace.getRoot();
        for (KeySpacePath path : dbPath.flatten()) {
            KeySpaceDirectory directory = path.getDirectory();
            current = current.getSubdirectory(directory.getName());
        }
        if (current.isLeaf() || current.getSubdirectories().stream().noneMatch(dr -> dr.getName().equals(schemaId))) {
            // this assignment has side effects, which is why we're doing it
            // TODO(bfines) this is probably not how we should be dealing with this
            addSchemaDirectory(current, schemaId);
        }
        return keySpace;
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

    // Add schema directory to the keySpace if needed
    private static synchronized KeySpaceDirectory addSchemaDirectory(@Nonnull KeySpaceDirectory dbDirectory, @Nonnull String schemaId) {
        if (dbDirectory.isLeaf() || dbDirectory.getSubdirectories().stream().noneMatch(dr -> dr.getName().equals(schemaId))) {
            dbDirectory.addSubdirectory(new KeySpaceDirectory(schemaId, KeySpaceDirectory.KeyType.STRING, schemaId));
        }
        return dbDirectory;
    }

    private KeySpaceUtils() {
    }

}

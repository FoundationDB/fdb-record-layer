/*
 * KeySpaceUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@API(API.Status.EXPERIMENTAL)
public final class KeySpaceUtils {

    @Nonnull
    public static URI pathToUri(@Nonnull KeySpacePath dbPath) {
        return URI.create(pathToUriList(dbPath).stream().collect(Collectors.joining("/", "/", "")));
    }

    @Nonnull
    private static List<String> pathToUriList(final @Nonnull KeySpacePath dbPath) {
        final List<String> asList = Stream.iterate(dbPath, Objects::nonNull, KeySpacePath::getParent)
                .map(KeySpaceUtils::toPathElement)
                .collect(Collectors.toCollection(ArrayList::new)); // guarantee mutability, for reverse
        Collections.reverse(asList);
        return asList;
    }

    private static String toPathElement(final KeySpacePath path) {
        if (path.getDirectory().getKeyType() != KeySpaceDirectory.KeyType.NULL) {
            return path.getValue().toString();
        } else {
            return "";
        }
    }

    public static String toPathString(KeySpacePath path) {
        return pathToUriList(path).stream().collect(Collectors.joining("/"));
    }

    @API(API.Status.INTERNAL)
    @Nonnull
    public static KeySpacePath toKeySpacePath(@Nonnull URI url, @Nonnull KeySpace keySpace) throws RelationalException {
        String path = getPath(url);
        if (path.length() < 1) {
            throw new RelationalException("<" + url + "> is an invalid database path", ErrorCode.INVALID_PATH);
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        // If the user specifies just `__SYS` that should point to the database, not the domain
        // or at least it did historically, as we develop the concept of domains more thoroughly we
        // may want to change this
        if (path.equals(RelationalKeyspaceProvider.SYS)) {
            return keySpace.path(RelationalKeyspaceProvider.SYS).add(RelationalKeyspaceProvider.SYS);
        }
        String[] pathElems = path.split("/");
        //TODO(bfines): this is super inefficient, we need to replace it with something more coherent
        if (path.endsWith("/")) {
            pathElems = Arrays.copyOf(pathElems, pathElems.length + 1);
            pathElems[pathElems.length - 1] = "";
        }
        final KeySpacePath thePath = matchPathToSubdirectories(keySpace, null, keySpace.getDirectories(),
                url, pathElems, 0);

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
    private static KeySpacePath matchPathToSubdirectories(final @Nonnull KeySpace keySpace,
                                                          final @Nullable KeySpacePath parentPath,
                                                          final @Nonnull List<KeySpaceDirectory> subdirectories,
                                                          final @Nonnull URI url,
                                                          final @Nonnull String[] pathElems,
                                                          final int position) throws RelationalException {
        KeySpacePath thePath = null;
        for (KeySpaceDirectory dir : subdirectories) {
            KeySpacePath path2 = matchPathToDirectory(keySpace, dir, parentPath, url, pathElems, position);

            if (path2 != null) {
                if (thePath != null) {
                    throw new RelationalException("<" + url + "> is ambigous", ErrorCode.INVALID_PATH)
                            .addContext("path1", thePath)
                            .addContext("path2", path2);
                } else {
                    thePath = path2;
                }
            }
        }
        return thePath;
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static KeySpacePath matchPathToDirectory(final @Nonnull KeySpace keySpace,
                                                     final @Nonnull KeySpaceDirectory directory,
                                                     final @Nullable KeySpacePath parentPath,
                                                     final @Nonnull URI url,
                                                     final @Nonnull String[] pathElems,
                                                     final int position) throws RelationalException {
        if (position >= pathElems.length) {
            throw new RelationalException("path is too deep", ErrorCode.INTERNAL_ERROR)
                    .addContext("position", position)
                    .addContext("path", Arrays.toString(pathElems));
        }
        String pathElem = pathElems[position];

        Object pathValue = null;
        switch (directory.getKeyType()) {
            case NULL:
                // empty string in URI represents NULL value, and non-empty value is invalid for directory with NULL KeyType
                if (!pathElem.isEmpty()) {
                    return null;
                }
                break;
            case STRING:
                pathValue = matchPathElementToString(directory, pathElem);
                if (pathValue == null) {
                    return null;
                }
                break;
            case LONG:
                if (directory instanceof DirectoryLayerDirectory) {
                    pathValue = matchPathElementToString(directory, pathElem);
                } else {
                    pathValue = matchPathElementToLong(directory, pathElem);
                }
                if (pathValue == null) {
                    return null;
                }
                break;
            default:
                throw new OperationUnsupportedException("Key Space paths only supported for NULL, LONG and STRING");
        }

        final KeySpacePath potentialPath = getSubPath(keySpace, parentPath, directory.getName(), pathValue);
        if (potentialPath == null) {
            return null;
        }

        if (directory.isLeaf()) {
            if (position == pathElems.length - 1) {
                return potentialPath; //we found the path!
            } else {
                return null;
            }
        }
        if (position + 1 == pathElems.length) {
            return potentialPath;
        }

        // search subdirectories
        return matchPathToSubdirectories(keySpace, potentialPath, directory.getSubdirectories(), url, pathElems, position + 1);
    }

    @Nullable
    private static String matchPathElementToString(final @Nonnull KeySpaceDirectory directory, final String pathElem) throws RelationalException {
        // the empty string maps to null, and cannot be a string
        if (directory.isConstant() && !Objects.equals(getConstantValue(directory), pathElem)) {
            return null;
        } else if (pathElem.isEmpty()) {
            return null;
        }
        return pathElem;
    }

    @Nullable
    private static Object matchPathElementToLong(final @Nonnull KeySpaceDirectory directory, final String pathElem) throws RelationalException {
        Long parsedLong = tryParseLong(pathElem);
        if (parsedLong == null) {
            return null;
        }
        final Object dirVal = getConstantValue(directory);
        if (directory.isConstant()) {
            if (dirVal instanceof Number) {
                // perhaps this should throw if dirVal is not a number
                if (((Number)dirVal).longValue() != parsedLong) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return parsedLong;
    }

    @Nullable
    private static Long tryParseLong(final String pathElem) {
        Long parsedLong;
        try {
            parsedLong = Long.parseLong(pathElem);
        } catch (NumberFormatException nfe) {
            //the field isn't a long, so can't match this directory
            return null;
        }
        return parsedLong;
    }

    @Nullable
    private static Object getConstantValue(final @Nonnull KeySpaceDirectory directory) throws RelationalException {
        Object dirVal = directory.getValue();
        if ("".equals(dirVal)) {
            throw new RelationalException("Directory contains constant empty string (\"\")", ErrorCode.INVALID_PATH)
                    .addContext("directory", directory.getName());
        }
        return dirVal;
    }

    @Nullable
    private static KeySpacePath getSubPath(final @Nonnull KeySpace keySpace, final @Nullable KeySpacePath parentPath, final String pathName, final Object pathValue) {
        try {
            if (parentPath == null) {
                return keySpace.path(pathName, pathValue);
            } else {
                return parentPath.add(pathName, pathValue);
            }
        } catch (NoSuchDirectoryException nsde) {
            //safety valve--shouldn't really ever be used, but if it happens we know it's not a valid path
            return null;
        }
    }

    private KeySpaceUtils() {
    }

}

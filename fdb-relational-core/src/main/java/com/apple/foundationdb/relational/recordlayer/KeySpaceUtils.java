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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class KeySpaceUtils {
    public static @Nonnull
    URI pathToURI(@Nonnull KeySpacePath dbPath) {
        final String path = dbPath.flatten().stream().map(keySpacePath -> {
            final KeySpaceDirectory directory = keySpacePath.getDirectory();
            switch (directory.getKeyType()) {
                case NULL:
                    return "";
                case BYTES:
                    //TODO(bfines) this is almost certainly not correct
                    return new String((byte[]) keySpacePath.getValue(), StandardCharsets.UTF_8);
                default:
                    return keySpacePath.getValue().toString();
            }
        }).reduce("", (left, right) -> left + "/" + right);
        return URI.create(path);
    }

    public static @Nonnull KeySpacePath uriToPath(@Nonnull URI url, @Nonnull KeySpace keySpace) {
        String[] pathElems = url.getPath().split("/");
        KeySpaceDirectory directory = keySpace.getRoot();
        KeySpacePath thePath = null;
        for(KeySpaceDirectory sub : directory.getSubdirectories()) {
            thePath = uriToPathRecursive(keySpace, sub, keySpace.path(sub.getName()), pathElems, 0);
        }

        if (thePath == null) {
            throw new RelationalException(url + " is an invalid database path", RelationalException.ErrorCode.INVALID_PATH);
        }

        return thePath;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static @Nullable
    KeySpacePath uriToPathRecursive(@Nonnull KeySpace keySpace,
                                    @Nonnull KeySpaceDirectory directory,
                                    KeySpacePath parentPath,
                                    @Nonnull String[] pathElems,
                                    int position) {
        if (position >= pathElems.length) {
            return parentPath;
        }
        String pathElem = pathElems[position];
        Object dirVal = directory.getValue();
        Object pathValue = null;
        switch (directory.getKeyType()) {
            case NULL:
                if (pathElem.length() > 0) {
                    return null; //this path doesn't match
                }
                break;
            case BYTES:
                //TODO(bfines) this may not be correct,depending on how charsets are used
                byte[] pathUtf8 = pathElem.getBytes(StandardCharsets.UTF_8);
                byte[] dirBytes = (byte[]) directory.getValue();
                if (!Arrays.equals(dirBytes, pathUtf8)) {
                    return null;
                }
                pathValue = pathUtf8;
                break;
            case STRING:
                pathValue = pathElem;
                if(Objects.equals(dirVal,KeySpaceDirectory.ANY_VALUE)){
                    break;
                }else if(!Objects.equals(dirVal,pathElem)){
                    return null;
                }
                break;
            case LONG:
                try {
                    long l = Long.parseLong(pathElem);
                    pathValue = l;
                    if(Objects.equals(dirVal,KeySpaceDirectory.ANY_VALUE)){
                       break;
                    } else if (!Objects.equals(dirVal, l)) {
                        return null;
                    }
                } catch (NumberFormatException nfe) {
                    //the field isn't a long, so can't match this directory
                    return null;
                }
                break;
            case FLOAT:
                try {
                    float l = Float.parseFloat(pathElem);
                    pathValue = l;
                    if (!Objects.equals(directory.getValue(), l)) {
                        return null;
                    }
                } catch (NumberFormatException nfe) {
                    //the field isn't a long, so can't match this directory
                    return null;
                }
                break;
            case DOUBLE:
                try {
                    double l = Double.parseDouble(pathElem);
                    pathValue = l;
                    if (!Objects.equals(directory.getValue(), l)) {
                        return null;
                    }
                } catch (NumberFormatException nfe) {
                    //the field isn't a long, so can't match this directory
                    return null;
                }
                break;
            case BOOLEAN:
                Boolean l = Boolean.parseBoolean(pathElem);
                pathValue = l;
                if (!Objects.equals(directory.getValue(), l)) {
                    return null;
                }
                break;
            case UUID:
                UUID uuid = UUID.fromString(pathElem);
                pathValue = uuid;
                if (!Objects.equals(directory.getValue(), uuid)) {
                    return null;
                }
                break;
        }

        if(directory.getParent()==keySpace.getRoot()){
            parentPath = keySpace.path(directory.getName(),pathValue);
        }else{
            parentPath = parentPath.add(directory.getName(),pathValue);
        }

        if(directory.isLeaf()){
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

}

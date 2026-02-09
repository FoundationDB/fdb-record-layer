/*
 * KeySpacePathSerializer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Class for serializing/deserializing between {@link DataInKeySpacePath} and {@link KeySpaceProto.DataInKeySpacePath}.
 * <p>
 *     This will serialize relative to a root path, such that the serialized form is relative to that path.
 *     <ul>
 *         <li>This reduces the size of the serialized data, which is particularly important when you have a lot of these.</li>
 *         <li>This allows the serialized form to be used as an intermediate if you have two identical sub-hierarchies
 *         in your {@link KeySpace}.</li>
 *     </ul>
 * </p>
 *
 */
@API(API.Status.EXPERIMENTAL)
public class KeySpacePathSerializer {

    @Nonnull
    private final List<KeySpacePath> root;

    /**
     * Constructor a new serializer for serializing relative to a given root path.
     * @param root a path which is a parent path of all the data you want to serialize
     */
    public KeySpacePathSerializer(@Nonnull final KeySpacePath root) {
        this.root = root.flatten();
    }

    /**
     * Serialize the given data relative to the root.
     * @param data a piece of data that is contained within the root
     * @return the data serialized relative to the root
     * @throws RecordCoreArgumentException if the given data is not contained within the root
     */
    @Nonnull
    public KeySpaceProto.DataInKeySpacePath serialize(@Nonnull DataInKeySpacePath data) {
        final List<KeySpacePath> dataPath = data.getPath().flatten();
        // two paths are only equal if their parents are equal, so we don't have to validate the whole prefix here
        if (dataPath.size() < root.size() ||
                !dataPath.get(root.size() - 1).equals(root.get(root.size() - 1))) {
            throw new RecordCoreArgumentException("Data is not contained within root path");
        }
        KeySpaceProto.DataInKeySpacePath.Builder builder = KeySpaceProto.DataInKeySpacePath.newBuilder();
        for (int i = root.size(); i < dataPath.size(); i++) {
            final KeySpacePath keySpacePath = dataPath.get(i);
            builder.addPath(serialize(keySpacePath));
        }
        if (data.getRemainder() != null) {
            builder.setRemainder(ByteString.copyFrom(data.getRemainder().pack()));
        }
        builder.setValue(data.getValue());
        return builder.build();
    }

    /**
     * Deserialize data relative to the root.
     * <p>
     *     Note: The given data does not need to have come from the same path, but all sub-paths must be valid.
     * </p>
     * @param proto a serialized form of {@link DataInKeySpacePath} as provided by {@link #serialize(DataInKeySpacePath)}
     * @return the deserialized data
     * @throws RecordCoreArgumentException if one of the path entries is not valid
     * @throws NoSuchDirectoryException if it refers to a directory that doesn't exist within the root
     */
    @Nonnull
    public DataInKeySpacePath deserialize(@Nonnull KeySpaceProto.DataInKeySpacePath proto) {
        // Start with the root path
        KeySpacePath path = root.get(root.size() - 1);

        // Add each path entry from the proto
        for (KeySpaceProto.KeySpacePathEntry entry : proto.getPathList()) {
            Object value = deserializeValue(entry);
            path.getDirectory().getSubdirectory(entry.getName()).validateValue(value);
            path = path.add(entry.getName(), value);
        }
        if (!path.getDirectory().isLeaf()) {
            throw new DataNotAtLeafException(path);
        }

        // Extract remainder if present
        Tuple remainder = null;
        if (proto.hasRemainder()) {
            remainder = Tuple.fromBytes(proto.getRemainder().toByteArray());
        }

        // Extract value
        if (!proto.hasValue()) {
            throw new RecordCoreArgumentException("Serialized data must have a value");
        }
        byte[] value = proto.getValue().toByteArray();

        return new DataInKeySpacePath(path, remainder, value);
    }

    @Nullable
    private static Object deserializeValue(@Nonnull KeySpaceProto.KeySpacePathEntry entry) {
        // Check which value field is set and return the appropriate value
        if (entry.hasNullValue()) {
            return null;
        } else if (entry.hasBytesValue()) {
            return entry.getBytesValue().toByteArray();
        } else if (entry.hasStringValue()) {
            return entry.getStringValue();
        } else if (entry.hasLongValue()) {
            return entry.getLongValue();
        } else if (entry.hasFloatValue()) {
            return entry.getFloatValue();
        } else if (entry.hasDoubleValue()) {
            return entry.getDoubleValue();
        } else if (entry.hasBooleanValue()) {
            return entry.getBooleanValue();
        } else if (entry.hasUuid()) {
            KeySpaceProto.KeySpacePathEntry.UUID uuidProto = entry.getUuid();
            return new UUID(uuidProto.getMostSignificantBits(), uuidProto.getLeastSignificantBits());
        } else {
            throw new RecordCoreArgumentException("KeySpacePathEntry has no value set")
                    .addLogInfo(LogMessageKeys.DIR_NAME, entry.getName());
        }
    }

    @Nonnull
    private static KeySpaceProto.KeySpacePathEntry serialize(@Nonnull final KeySpacePath keySpacePath) {
        final Object value = keySpacePath.getValue();
        // Use typeOf to get the actual runtime type of the value, rather than the directory's declared keyType.
        // This is important for DirectoryLayerDirectory, which has keyType LONG but typically stores String values.
        // If we ever support something that takes a value that is not supported via KeyType, we'll need to remove this
        // dependency, but right now it is convenient to reuse that enum.
        final KeySpaceDirectory.KeyType keyType = value == null
                ? KeySpaceDirectory.KeyType.NULL
                : KeySpaceDirectory.KeyType.typeOf(value);

        KeySpaceProto.KeySpacePathEntry.Builder builder = KeySpaceProto.KeySpacePathEntry.newBuilder()
                .setName(keySpacePath.getDirectoryName());
        try {
            switch (keyType) {
                case NULL:
                    builder.setNullValue(true);
                    break;
                case BYTES:
                    builder.setBytesValue(ByteString.copyFrom((byte[])value));
                    break;
                case STRING:
                    builder.setStringValue((String)value);
                    break;
                case LONG:
                    if (value instanceof Integer) {
                        builder.setLongValue(((Integer)value).longValue());
                    } else {
                        builder.setLongValue((Long)value);
                    }
                    break;
                case FLOAT:
                    builder.setFloatValue((Float)value);
                    break;
                case DOUBLE:
                    builder.setDoubleValue((Double)value);
                    break;
                case BOOLEAN:
                    builder.setBooleanValue((Boolean)value);
                    break;
                case UUID:
                    final UUID uuid = (UUID)value;
                    builder.getUuidBuilder()
                            .setLeastSignificantBits(uuid.getLeastSignificantBits())
                            .setMostSignificantBits(uuid.getMostSignificantBits());
                    break;
                default:
                    throw new IllegalStateException("Unexpected value type: " + keyType);
            }
        } catch (ClassCastException e) {
            throw new RecordCoreArgumentException("KeySpacePath has incorrect value type", e)
                    .addLogInfo(
                            LogMessageKeys.DIR_NAME, keySpacePath.getDirectoryName(),
                            LogMessageKeys.EXPECTED_TYPE, keyType,
                            LogMessageKeys.ACTUAL, value);

        }
        return builder.build();
    }

}

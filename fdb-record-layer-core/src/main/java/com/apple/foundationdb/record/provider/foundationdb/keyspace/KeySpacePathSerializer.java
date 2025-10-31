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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.UUID;

/**
 * Class for serializing/deserializing between {@link DataInKeySpacePath} and {@link KeySpaceProto.DataInKeySpacePath}.
 * <p>
 *     This will serialize relative to a root path, such that the serialized form is relative to that path. This can be
 *     useful to both:
 *     <ul>
 *         <li>Reduce the size of the serialized data, particularly when you have a lot of these.</li>
 *         <li>Allowing as an intermediate if you have two identical sub-hierarchies in your {@link KeySpace}.</li>
 *     </ul>
 * </p>
 *
 */
public class KeySpacePathSerializer {

    private final List<KeySpacePath> root;

    public KeySpacePathSerializer(final KeySpacePath root) {
        this.root = root.flatten();
    }

    public ByteString serialize(DataInKeySpacePath data) {
        KeySpaceProto.DataInKeySpacePath.Builder builder = KeySpaceProto.DataInKeySpacePath.newBuilder();
        final List<KeySpacePath> dataPath = data.getPath().flatten();
        if (dataPath.size() < root.size() ||
                !dataPath.get(root.size() - 1).equals(root.get(root.size() - 1))) {
            throw new RecordCoreArgumentException("Data is not contained within root path");
        }
        for (int i = root.size(); i < dataPath.size(); i++) {
            final KeySpacePath keySpacePath = dataPath.get(i);
            builder.addPath(serialize(keySpacePath));
        }
        if (data.getRemainder() != null) {
            builder.setRemainder(ByteString.copyFrom(data.getRemainder().pack()));
        }
        builder.setValue(ByteString.copyFrom(data.getValue()));
        return builder.build().toByteString();
    }

    private static KeySpaceProto.KeySpacePathEntry serialize(final KeySpacePath keySpacePath) {
        KeySpaceProto.KeySpacePathEntry.Builder builder = KeySpaceProto.KeySpacePathEntry.newBuilder()
                .setName(keySpacePath.getDirectoryName());
        final Object value = keySpacePath.getValue();
        final KeySpaceDirectory.KeyType keyType = keySpacePath.getDirectory().getKeyType();
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
                    builder.setLongValue((Long)value);
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
                    throw new IllegalStateException("Unexpected value: " + keyType);
            }
        } catch (NullPointerException | ClassCastException e) {
            throw new RecordCoreArgumentException("KeySpacePath has incorrect value")
                    .addLogInfo(
                            LogMessageKeys.DIR_NAME, keySpacePath.getDirectoryName(),
                            LogMessageKeys.EXPECTED_TYPE, keyType,
                            LogMessageKeys.ACTUAL, value);

        }
        return builder.build();
    }

}

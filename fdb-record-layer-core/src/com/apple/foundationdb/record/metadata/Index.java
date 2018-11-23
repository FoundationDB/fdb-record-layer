/*
 * Index.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;

/**
 * Meta-data for a secondary index.
 *
 * @see com.apple.foundationdb.record.RecordMetaDataBuilder#addIndex
 */
@API(API.Status.MAINTAINED)
public class Index {
    @Nonnull
    private final String name;
    @Nonnull
    private final String type;
    @Nonnull
    private final Map<String, String> options;
    @Nonnull
    private final KeyExpression rootExpression;
    @Nullable
    private int[] primaryKeyComponentPositions;
    @Nonnull
    private Object subspaceKey;
    private int addedVersion;
    private int lastModifiedVersion;

    public static Object decodeSubspaceKey(@Nonnull ByteString bytes) {
        Tuple tuple = Tuple.fromBytes(bytes.toByteArray());
        if (tuple.size() != 1) {
            throw new RecordCoreException("subspace key must encode a single item tuple");
        }
        return tuple.get(0);
    }

    public static final KeyExpression EMPTY_VALUE = EmptyKeyExpression.EMPTY;

    /**
     * Construct new index meta-data.
     * @param name the name of the index, which is unique for the whole meta-data
     * @param rootExpression the key expression for the index, such as what field(s) to index
     * @param type the type of index
     * @param options additional options, which may be type-specific
     * @see IndexTypes
     */
    public Index(@Nonnull String name,
                 @Nonnull KeyExpression rootExpression,
                 @Nonnull String type,
                 @Nonnull Map<String, String> options) {
        this.name = name;
        this.rootExpression = rootExpression;
        this.type = type;
        this.options = options;
        this.subspaceKey = name;
        this.lastModifiedVersion = 0;
    }

    public Index(@Nonnull String name,
                 @Nonnull KeyExpression rootExpression,
                 @Nonnull KeyExpression valueExpression,
                 @Nonnull String type,
                 @Nonnull Map<String, String> options) {
        this(name, toKeyWithValueExpression(rootExpression, valueExpression), type, options);
    }

    public Index(@Nonnull String name,
                 @Nonnull KeyExpression rootExpression,
                 @Nonnull String type) {
        this(name, rootExpression, type, IndexOptions.EMPTY_OPTIONS);
    }


    public Index(@Nonnull String name,
                 @Nonnull KeyExpression rootExpression) {
        this(name, rootExpression, IndexTypes.VALUE);
    }

    public Index(@Nonnull String name, @Nonnull String first) {
        this(name, Key.Expressions.field(first));
    }

    public Index(@Nonnull String name, @Nonnull String first, @Nonnull String second, @Nonnull String... rest) {
        this(name, Key.Expressions.concatenateFields(first, second, rest));
    }

    /**
     * Copy constructor. This will create an index that is identical to the current <code>Index</code>
     * instance.
     * @param orig original index to copy
     */
    public Index(@Nonnull Index orig) {
        this(orig.name, orig.rootExpression, orig.type, new HashMap<>(orig.options));
        if (orig.primaryKeyComponentPositions != null) {
            this.primaryKeyComponentPositions = Arrays.copyOf(orig.primaryKeyComponentPositions, orig.primaryKeyComponentPositions.length);
        } else {
            this.primaryKeyComponentPositions = null;
        }
        this.subspaceKey = Tuple.fromBytes(Tuple.from(orig.subspaceKey).pack()).get(0);
        this.addedVersion = orig.addedVersion;
        this.lastModifiedVersion = orig.lastModifiedVersion;
    }

    @SuppressWarnings({"deprecation","squid:CallToDeprecatedMethod"}) // Old (deprecated) index type needs grouping compatibility
    public Index(@Nonnull RecordMetaDataProto.Index proto) throws KeyExpression.DeserializationException {
        name = proto.getName();
        // Compatibility with old serialized metadata.
        // TODO: Can be removed when / if all metadata has been regenerated.
        if (proto.hasIndexType()) {
            type = indexTypeToType(proto.getIndexType());
            options = indexTypeToOptions(proto.getIndexType());
        } else {
            type = proto.hasType() ? proto.getType() : IndexTypes.VALUE;
            options = buildOptions(proto.getOptionsList(), false);
        }
        KeyExpression expr = KeyExpression.fromProto(proto.getRootExpression());
        if (!(expr instanceof GroupingKeyExpression) &&
                (type.equals(IndexTypes.RANK) ||
                 type.equals(IndexTypes.COUNT) ||
                 type.equals(IndexTypes.MAX_EVER) ||
                 type.equals(IndexTypes.MIN_EVER) ||
                 type.equals(IndexTypes.SUM))) {
            expr = new GroupingKeyExpression(expr, type.equals(IndexTypes.COUNT) ? expr.getColumnSize() : 1);
        }

        if (proto.hasValueExpression()) {
            KeyExpression value = KeyExpression.fromProto(proto.getValueExpression());
            rootExpression = toKeyWithValueExpression(expr, value);
        } else {
            rootExpression = expr;
        }

        if (proto.hasSubspaceKey()) {
            subspaceKey = decodeSubspaceKey(proto.getSubspaceKey());
        } else {
            subspaceKey = name;
        }
        if (proto.hasAddedVersion()) {
            addedVersion = proto.getAddedVersion();
        } else {
            // Indexes from before this field existed need to appear to be old.
            // But addIndexCommon will set to lastModifiedVersion if kept at 0, so set to first valid version.
            addedVersion = 1;
        }
        if (proto.hasLastModifiedVersion()) {
            lastModifiedVersion = proto.getLastModifiedVersion();
        }
    }

    @Nonnull
    private static KeyExpression toKeyWithValueExpression(@Nonnull KeyExpression rootExpression,
                                                          @Nonnull KeyExpression valueExpression) {
        if (valueExpression.getColumnSize() == 0) {
            return rootExpression;
        }
        return keyWithValue(concat(rootExpression, valueExpression), rootExpression.getColumnSize());
    }

    public static Map<String, String> buildOptions(List<RecordMetaDataProto.Index.Option> optionList, boolean addUnique) {
        if (optionList.isEmpty() && !addUnique) {
            return IndexOptions.EMPTY_OPTIONS;
        } else {
            Map<String, String> options = new TreeMap<>();
            if (addUnique) {
                options.put(IndexOptions.UNIQUE_OPTION, Boolean.TRUE.toString());
            }
            for (RecordMetaDataProto.Index.Option option : optionList) {
                options.put(option.getKey(), option.getValue());
            }
            return options;
        }
    }

    public static String indexTypeToType(RecordMetaDataProto.Index.Type indexType) {
        switch (indexType) {
            case RANK:
            case RANK_UNIQUE:
                return IndexTypes.RANK;
            case INDEX:
            case UNIQUE:
            default:
                return IndexTypes.VALUE;
        }
    }

    public static Map<String, String> indexTypeToOptions(RecordMetaDataProto.Index.Type indexType) {
        switch (indexType) {
            case UNIQUE:
            case RANK_UNIQUE:
                return IndexOptions.UNIQUE_OPTIONS;
            case INDEX:
            case RANK:
            default:
                return IndexOptions.EMPTY_OPTIONS;
        }
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public String getType() {
        return type;
    }

    @Nonnull
    public Map<String, String> getOptions() {
        return options;
    }

    @Nullable
    public String getOption(@Nonnull String key) {
        return options.get(key);
    }

    public boolean getBooleanOption(@Nonnull String key, boolean defaultValue) {
        final String option = getOption(key);
        if (option == null) {
            return defaultValue;
        } else {
            return Boolean.valueOf(option);
        }
    }

    @Nonnull
    public KeyExpression getRootExpression() {
        return rootExpression;
    }

    /**
     * Whether this index should have the property that any two records
     * with different primary keys should have different values for this
     * index. The default value for this is <code>false</code> if the
     * option is not set explicitly.
     * @return the value of the "unique" option
     */
    public boolean isUnique() {
        return getBooleanOption(IndexOptions.UNIQUE_OPTION, false);
    }

    public Object getSubspaceKey() {
        return subspaceKey;
    }

    public void setSubspaceKey(Object subspaceKey) {
        this.subspaceKey = subspaceKey;
    }

    @Nullable
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public int[] getPrimaryKeyComponentPositions() {
        return primaryKeyComponentPositions;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public void setPrimaryKeyComponentPositions(int[] primaryKeyComponentPositions) {
        this.primaryKeyComponentPositions = primaryKeyComponentPositions;
    }

    public void trimPrimaryKey(List<?> primaryKeys) {
        if (primaryKeyComponentPositions != null) {
            for (int i = primaryKeyComponentPositions.length - 1; i >= 0; i--) {
                if (primaryKeyComponentPositions[i] >= 0) {
                    primaryKeys.remove(i);
                }
            }
        }
    }

    /**
     * Get the number of indexed value columns.
     * @return the number of columns
     */
    public int getColumnSize() {
        return rootExpression.getColumnSize();
    }

    /**
     * The number of columns stored for an index entry.
     * Does not count primary key columns that are part of the indexed value, which do not need to be duplicated.
     * @param primaryKey the primary key for eliminating duplicates
     * @return the size of an index entry
     */
    public int getEntrySize(KeyExpression primaryKey) {
        int total = getColumnSize() + primaryKey.getColumnSize();
        if (primaryKeyComponentPositions != null) {
            for (int pos : primaryKeyComponentPositions) {
                if (pos >= 0) {
                    total--;
                }
            }
        }
        return total;
    }

    public int getAddedVersion() {
        return addedVersion;
    }

    public void setAddedVersion(int addedVersion) {
        this.addedVersion = addedVersion;
    }

    public int getLastModifiedVersion() {
        return lastModifiedVersion;
    }

    public void setLastModifiedVersion(int lastModifiedVersion) {
        this.lastModifiedVersion = lastModifiedVersion;
    }

    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor recordType) {
        return rootExpression.validate(recordType);
    }

    @Nonnull
    public RecordMetaDataProto.Index toProto() throws KeyExpression.SerializationException {
        final RecordMetaDataProto.Index.Builder builder = RecordMetaDataProto.Index.newBuilder();
        builder.setName(name);
        builder.setRootExpression(rootExpression.toKeyExpression());
        builder.setType(type);
        for (Map.Entry<String, String> entry : options.entrySet()) {
            builder.addOptionsBuilder().setKey(entry.getKey()).setValue(entry.getValue());
        }
        if (!name.equals(subspaceKey)) {
            builder.setSubspaceKey(ByteString.copyFrom(Tuple.from(subspaceKey).pack()));
        }
        if (addedVersion > 0) {
            builder.setAddedVersion(addedVersion);
        }
        if (lastModifiedVersion > 0) {
            builder.setLastModifiedVersion(lastModifiedVersion);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Index {'").append(name).append("'");
        if (!type.equals(IndexTypes.VALUE)) {
            str.append(", ").append(type);
        }
        str.append("}");
        if (lastModifiedVersion > 0) {
            str.append("#").append(lastModifiedVersion);
        }
        return str.toString();
    }

}

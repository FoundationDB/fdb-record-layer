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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;

/**
 * Meta-data for a secondary index.
 *
 * @see com.apple.foundationdb.record.RecordMetaDataBuilder#addIndex
 */
@API(API.Status.UNSTABLE)
public class Index {
    @Nonnull
    public static final KeyExpression EMPTY_VALUE = EmptyKeyExpression.EMPTY;

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
    private boolean useExplicitSubspaceKey = false;
    private int addedVersion;
    private int lastModifiedVersion;
    @Nullable
    private final IndexPredicate predicate;

    public static Object decodeSubspaceKey(@Nonnull ByteString bytes) {
        Tuple tuple = Tuple.fromBytes(bytes.toByteArray());
        if (tuple.size() != 1) {
            throw new RecordCoreException("subspace key must encode a single item tuple");
        }
        return tuple.get(0);
    }

    @Nonnull
    private static Object normalizeSubspaceKey(@Nonnull String name, @Nonnull Object subspaceKey) {
        Object normalizedKey = TupleTypeUtil.toTupleEquivalentValue(subspaceKey);
        if (normalizedKey == null) {
            throw new RecordCoreArgumentException("Index subspace key cannot be null",
                    LogMessageKeys.INDEX_NAME, name,
                    LogMessageKeys.SUBSPACE_KEY, subspaceKey);
        }
        return normalizedKey;
    }

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
        this(name, rootExpression, type, options, null);
    }

    /**
     * Construct new index meta-data.
     * @param name the name of the index, which is unique for the whole meta-data
     * @param rootExpression the key expression for the index, such as what field(s) to index
     * @param type the type of index
     * @param options additional options, which may be type-specific
     * @param predicate index predicate, for sparse indexes, can be null.
     * @see IndexTypes
     */
    public Index(@Nonnull String name,
                 @Nonnull KeyExpression rootExpression,
                 @Nonnull String type,
                 @Nonnull Map<String, String> options,
                 @Nullable IndexPredicate predicate) {
        this.name = name;
        this.rootExpression = rootExpression;
        this.type = type;
        this.options = ImmutableMap.copyOf(options);
        this.subspaceKey = normalizeSubspaceKey(name, name);
        this.lastModifiedVersion = 0;
        this.predicate = predicate;
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
        this(orig, orig.predicate);
    }

    /**
     * Copy constructor. This will create an index that is identical to the current <code>Index</code> with a given
     * <code>IndexPredicate</code>
     * @param orig original index to copy
     * @param predicate the index predicate
     */
    public Index(@Nonnull Index orig, @Nullable final IndexPredicate predicate) {
        this(orig.name, orig.rootExpression, orig.type, ImmutableMap.copyOf(orig.options), predicate);
        if (orig.primaryKeyComponentPositions != null) {
            this.primaryKeyComponentPositions = Arrays.copyOf(orig.primaryKeyComponentPositions, orig.primaryKeyComponentPositions.length);
        } else {
            this.primaryKeyComponentPositions = null;
        }
        this.subspaceKey = normalizeSubspaceKey(name, orig.subspaceKey);
        this.useExplicitSubspaceKey = orig.useExplicitSubspaceKey;
        this.addedVersion = orig.addedVersion;
        this.lastModifiedVersion = orig.lastModifiedVersion;
    }

    @SuppressWarnings({"deprecation", "squid:CallToDeprecatedMethod", "java:S3776"}) // Old (deprecated) index type needs grouping compatibility
    @SpotBugsSuppressWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
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
            expr = toKeyWithValueExpression(expr, value);
        }

        rootExpression = expr;
        if (proto.hasSubspaceKey()) {
            this.subspaceKey = normalizeSubspaceKey(name, decodeSubspaceKey(proto.getSubspaceKey()));
        } else {
            this.subspaceKey = normalizeSubspaceKey(name, name);
        }
        this.useExplicitSubspaceKey = true;
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
        if (proto.hasPredicate()) {
            this.predicate = IndexPredicate.fromProto(proto.getPredicate());
        } else {
            this.predicate = null;
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
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            if (addUnique) {
                builder.put(IndexOptions.UNIQUE_OPTION, Boolean.TRUE.toString());
            }
            for (RecordMetaDataProto.Index.Option option : optionList) {
                builder.put(option.getKey(), option.getValue());
            }
            return builder.build();
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

    /**
     * Returns the type of the index.
     *
     * @return the type of the index.
     * @see IndexTypes
     */
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

    /**
     * Get a list of index names that are replacing this index. If the list is non-empty, it should contain
     * a list of other indexes in the meta-data. When all of those indexes are built on a store, this
     * index will be marked as {@link com.apple.foundationdb.record.IndexState#DISABLED}, so it will no
     * longer be maintained and its data will be deleted. See the {@link IndexOptions#REPLACED_BY_OPTION_PREFIX}
     * for more details. If the returned list is empty, then this index is not being replaced by any other
     * indexes.
     *
     * @return either the empty list or a list of indexes that when built should cause this index to be removed
     * @see IndexOptions#REPLACED_BY_OPTION_PREFIX
     */
    @Nonnull
    public List<String> getReplacedByIndexNames() {
        ImmutableList.Builder<String> replacedByIndexNames = ImmutableList.builder();
        for (Map.Entry<String, String> option : getOptions().entrySet()) {
            if (option.getKey().startsWith(IndexOptions.REPLACED_BY_OPTION_PREFIX)) {
                replacedByIndexNames.add(option.getValue());
            }
        }
        return replacedByIndexNames.build();
    }

    /**
     * Get the key used to determine this index's subspace prefix. All of the index's
     * data will live within a subspace constructed by adding this key to a record store's
     * subspace for secondary indexes. Each index within a given meta-data definition must have a
     * unique subspace key. By default, this is equal to the index's name, but alternative keys
     * can be set by calling {@link #setSubspaceKey(Object)}.
     *
     * @return the key used to determine this index's subspace prefix
     * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#indexSubspace(Index)
     */
    @Nonnull
    public Object getSubspaceKey() {
        return subspaceKey;
    }

    /**
     * Get a {@link Tuple}-encodable version of the {@linkplain #getSubspaceKey() subspace key} of this index.
     * As the subspace key is not guaranteed to be of a {@code Tuple}-encodable type on its own, this
     * method is preferred over {@link #getSubspaceKey()} if one is constructing a key to read or write data
     * from the database.
     *
     * @return a {@link Tuple}-encodable version of index subspace key
     */
    @Nonnull
    public Object getSubspaceTupleKey() {
        return TupleTypeUtil.toTupleAppropriateValue(subspaceKey);
    }

    /**
     * Set the key used to determine this index's subspace prefix. This value must be
     * unique for each index within a given meta-data definition and must be serializable
     * using a FoundationDB {@link Tuple}. As this value will prefix all keys used by the
     * index, it is generally advisable that this key have a compact serialized form. For
     * example, integers are encoded by the {@code Tuple} layer using a variable length
     * encoding scheme that makes them a natural choice for this key.
     *
     * <p>
     * It is important that once an index has data that its subspace key not change. If
     * one wishes to change the key, the guidance would be to create a new index with the
     * same definition but at the new key. Then that index can be built using the
     * {@link com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer OnlineIndexer}.
     * When that index has been fully built, the original index can be safely dropped.
     * </p>
     *
     * @param subspaceKey the key used to determine this index's subspace prefix
     * @see #getSubspaceKey()
     */
    public void setSubspaceKey(@Nonnull Object subspaceKey) {
        useExplicitSubspaceKey = true;
        this.subspaceKey = normalizeSubspaceKey(name, subspaceKey);
    }

    /**
     * Checks whether the subspace key was set using {@link #setSubspaceKey(Object)}.
     * @return {@code true} if the subspace key was set using {@code #setSubspaceKey(Object)}
     */
    public boolean hasExplicitSubspaceKey() {
        return useExplicitSubspaceKey;
    }

    /**
     * Get the positions of the primary key components within the index key.
     * This is used if the index key and the primary key expressions share fields to avoid
     * duplicating the shared fields when writing serializing the index entry. This
     * might return {@code null} if there are no common fields used by both the
     * index key and the primary key. Otherwise, it will return an array that is the
     * same length as the {@linkplain KeyExpression#getColumnSize() column size} of the
     * primary key. Each position in the array should either contain the index in the
     * index key where one can find the value of the primary key component in that
     * position or a negative value to indicate that that column is not found in the
     * index key.
     *
     * <p>
     * For example, suppose one had an index defined on a record type with a primary
     * key of {@code Key.Expressions.concatenateFields("a", "b")} and suppose the index
     * was defined on {@code Key.Expressions.concatenateFields("a", "c")}. A na&iuml;ve
     * approach might serialize index key tuples of the form {@code (a, c, a, b)}
     * by concatenating the index key (i.e., {@code (a, c)}) with the primary key
     * (i.e., {@code (a, b)}). However, as the first component of the primary key tuple
     * can be found within the index's tuple, indexes will instead serialize index
     * key tuples of the form {@code (a, c, b)}. This function will then return the array
     * {@code {0, -1}} to indicate that the first component of the primary key can be
     * found at position 0 in the index entry key but the second component is found after
     * the index key data (which is the default location).
     * </p>
     *
     * <p>
     * This method should generally not be called by users outside of the Record Layer.
     * For the most part, it should be sufficient for index maintainers to call
     * {@link #getEntryPrimaryKey(Tuple)}
     * to determine the primary key of a record from an index entry and
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase#indexEntryKey(Index, Tuple, Tuple) FDBRecordStoreBase.indexEntryKey()}
     * to determine the index entry given a record's primary key.
     * </p>
     *
     * <p>
     * At the moment, this optimization is not used with multi-type or universal indexes.
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/93">Issue #93</a>
     * for more details.
     * </p>
     *
     * @return the positions of primary key components within the index key or {@code null}
     */
    @Nullable
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public int[] getPrimaryKeyComponentPositions() {
        return primaryKeyComponentPositions;
    }

    /**
     * Set the positions of primary key components within the index key.
     * This generally should not be called by users outside of the Record Layer
     * as it can affect how data are serialized to disk in incompatible ways.
     *
     * @param primaryKeyComponentPositions the positions of primary key components within the index key
     * @see #getPrimaryKeyComponentPositions()
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public void setPrimaryKeyComponentPositions(int[] primaryKeyComponentPositions) {
        this.primaryKeyComponentPositions = primaryKeyComponentPositions;
    }

    @API(API.Status.INTERNAL)
    public void trimPrimaryKey(List<?> primaryKeys) {
        if (primaryKeyComponentPositions != null) {
            for (int i = primaryKeyComponentPositions.length - 1; i >= 0; i--) {
                if (primaryKeyComponentPositions[i] >= 0) {
                    primaryKeys.remove(i);
                }
            }
        }
    }

    @API(API.Status.INTERNAL)
    public BitSet getCoveredPrimaryKeyPositions() {
        final BitSet resultSet;
        if (primaryKeyComponentPositions != null) {
            resultSet = new BitSet(primaryKeyComponentPositions.length);
            for (int i = 0; i < primaryKeyComponentPositions.length; i++) {
                if (primaryKeyComponentPositions[i] >= 0) {
                    resultSet.set(i);
                }
            }
        } else {
            resultSet = new BitSet();
        }
        return resultSet;
    }
    
    /**
     * Return whether this index has non-default primary key component positions.
     * In particular, this will return {@code true} if it does something more advanced than appending
     * all of the primary key fields to the end of the index key. If this method returns
     * {@code true}, then {@link #getPrimaryKeyComponentPositions()} will return a non-null
     * array and at least one entry will be non-negative.
     *
     * @return whether this index has non-default primary key component positions
     * @see #getPrimaryKeyComponentPositions()
     */
    boolean hasPrimaryKeyComponentPositions() {
        return this.primaryKeyComponentPositions != null && IntStream.of(this.primaryKeyComponentPositions).anyMatch(i -> i >= 0);
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

    /**
     * Get the primary key portion of an index entry.
     * @param entry the index entry
     * @return the primary key extracted from the entry
     */
    @Nonnull
    public Tuple getEntryPrimaryKey(@Nonnull Tuple entry) {
        List<Object> entryKeys = entry.getItems();
        List<Object> primaryKeys;
        if (primaryKeyComponentPositions == null) {
            primaryKeys = entryKeys.subList(getColumnSize(), entryKeys.size());
        } else {
            primaryKeys = new ArrayList<>(primaryKeyComponentPositions.length);
            int after = getColumnSize();
            for (int position : primaryKeyComponentPositions) {
                primaryKeys.add(entryKeys.get(position < 0 ? after++ : position));
            }
        }
        return Tuple.fromList(primaryKeys);
    }

    /**
     * Get the primary key positions of an index entry.
     * @param primaryKeyLength the number of elements in the primary key for the record
     * @return a list of the primary key positions for an entry of this index
     */
    @Nonnull
    public List<Integer> getEntryPrimaryKeyPositions(int primaryKeyLength) {
        List<Integer> primaryKeys = new ArrayList<>(primaryKeyLength);
        int columnSize = getColumnSize();
        if (primaryKeyComponentPositions == null) {
            for (int i = columnSize; i < (columnSize + primaryKeyLength); i++ ) {
                primaryKeys.add(i);
            }
        } else {
            int after = columnSize;
            for (int position : primaryKeyComponentPositions) {
                primaryKeys.add((position < 0) ? after++ : position);
            }
        }
        return primaryKeys;
    }

    /**
     * Get the version at which the index was first added.
     * @return the added version
     */
    public int getAddedVersion() {
        return addedVersion;
    }

    /**
     * Set the version at which the index was first added.
     * @param addedVersion the added version
     */
    public void setAddedVersion(int addedVersion) {
        this.addedVersion = addedVersion;
    }

    /**
     * Get the version at which the index was changed.
     *
     * Any record store older than this will need to have the index rebuilt.
     * @return the last modified version
     */
    public int getLastModifiedVersion() {
        return lastModifiedVersion;
    }

    /**
     * Returns the predicate associated with the index in case of a filtered (sparse) index.
     *
     * @return The predicate associated with the index if the index is filtered (sparse), otherwise {@code null}.
     */
    @Nullable
    public IndexPredicate getPredicate() {
        return predicate;
    }

    /**
     * Checks whether a predicate is defined on the index.
     *
     * @return {@code true} if the index has a predicate, otherwise {@code false}.
     */
    public boolean hasPredicate() {
        return predicate != null;
    }

    /**
     * Set the version at which the index was changed.
     * @param lastModifiedVersion the last modified version
     */
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
        builder.setSubspaceKey(ZeroCopyByteString.wrap(Tuple.from(subspaceKey).pack()));
        if (addedVersion > 0) {
            builder.setAddedVersion(addedVersion);
        }
        if (lastModifiedVersion > 0) {
            builder.setLastModifiedVersion(lastModifiedVersion);
        }
        if (predicate != null) {
            builder.setPredicate(predicate.toProto());
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
        if (predicate != null) {
            str.append("where ").append(predicate);
        }
        return str.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }
        Index that = (Index) o;
        return this.name.equals(that.name)
                && this.type.equals(that.type)
                && this.rootExpression.equals(that.rootExpression)
                && this.subspaceKey.equals(that.subspaceKey)
                && this.addedVersion == that.addedVersion
                && this.lastModifiedVersion == that.lastModifiedVersion
                && Arrays.equals(this.primaryKeyComponentPositions, that.primaryKeyComponentPositions)
                && this.options.equals(that.options)
                && Objects.equals(this.predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        // Within the context of a single RecordMetaData, this should be sufficient
        return Objects.hash(name, type);
    }
}

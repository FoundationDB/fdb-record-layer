/*
 * KeySpaceTreeResolver.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolve trees of key space paths and record store descendants.
 */
public class KeySpaceTreeResolver {
    private static final CompletableFuture<Resolved> UNRESOLVED = CompletableFuture.completedFuture(null);
    private static final CompletableFuture<RecordMetaData> NO_META_DATA = CompletableFuture.completedFuture(null);

    public KeySpaceTreeResolver() {
    }

    @Nullable
    public FDBRecordContext getContext() {
        return null;
    }

    /**
     * The resolved interpretation of the node value.
     *
     * Extend this for application-specific node resolution.
     */
    public abstract static class Resolved {
        @Nullable
        private final Resolved parent;

        protected Resolved(@Nullable Resolved parent) {
            this.parent = parent;
        }

        @Nullable
        public Resolved getParent() {
            return parent;
        }

        @Nonnull
        public abstract String getName();

        @Nullable
        public abstract Object getLogicalValue();

        @Nullable
        public abstract Object getResolvedValue();

        @Nullable
        @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract")
        public KeySpaceDirectory getDirectory() {
            return null;
        }

        @Nullable
        @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract")
        public ResolvedKeySpacePath getResolvedPath() {
            return null;
        }

        @Nullable
        public KeySpacePath getPath() {
            final ResolvedKeySpacePath resolved = getResolvedPath();
            return resolved == null ? null : resolved.toPath();
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder();
            appendString(str);
            return str.toString();
        }

        public String toPathString() {
            StringBuilder str = new StringBuilder();
            appendPathString(str);
            return str.toString();
        }

        public StringBuilder appendString(StringBuilder str) {
            str.append(getName()).append(':');
            ResolvedKeySpacePath.appendValue(str, getLogicalValue());
            if (!Objects.equals(getLogicalValue(), getResolvedValue())) {
                str.append('[');
                ResolvedKeySpacePath.appendValue(str, getResolvedValue());
                str.append(']');
            }
            return str;
        }

        public StringBuilder appendPathString(StringBuilder str) {
            if (parent != null) {
                parent.appendPathString(str);
            }
            str.append('/');
            appendString(str);
            return str;
        }
    }

    /**
     * {@link Resolved} to a {@link KeySpace} root.
     */
    public static class ResolvedRoot extends Resolved {
        @Nonnull
        private final KeySpaceDirectory rootDirectory;

        protected ResolvedRoot(@Nullable Resolved parent, @Nonnull KeySpaceDirectory rootDirectory) {
            super(parent);
            this.rootDirectory = rootDirectory;
        }

        public ResolvedRoot(@Nonnull KeySpace keySpace) {
            this(null, keySpace.getRoot());
        }

        @Nonnull
        @Override
        public String getName() {
            return rootDirectory.getName();
        }

        @Nullable
        @Override
        public Object getLogicalValue() {
            return null;
        }

        @Nullable
        @Override
        public Object getResolvedValue() {
            return null;
        }

        @Nullable
        @Override
        public KeySpaceDirectory getDirectory() {
            return rootDirectory;
        }

        @Override
        public StringBuilder appendPathString(final StringBuilder str) {
            return str; // No need for "/root:null"
        }
    }

    /**
     * {@link ResolvedRoot} with a prefix object.
     */
    public static class ResolvedPrefixRoot extends ResolvedRoot {
        @Nonnull
        private final Object prefix;

        public ResolvedPrefixRoot(@Nonnull Resolved parent, @Nonnull Object prefix) {
            super(parent, parent.getDirectory());
            this.prefix = prefix;
        }

        @Override
        public String toString() {
            return prefix.toString();
        }
    }

    /**
     * {@link Resolved} to a {@link ResolvedKeySpacePath}.
     */
    public static class ResolvedPath extends Resolved {
        @Nonnull
        private final ResolvedKeySpacePath resolvedKeySpacePath;

        public ResolvedPath(@Nonnull Resolved parent, @Nonnull ResolvedKeySpacePath resolvedKeySpacePath) {
            super(parent);
            this.resolvedKeySpacePath = resolvedKeySpacePath;
        }

        @Nullable
        @Override
        public KeySpaceDirectory getDirectory() {
            return resolvedKeySpacePath.getDirectory();
        }

        @Nullable
        @Override
        public ResolvedKeySpacePath getResolvedPath() {
            return resolvedKeySpacePath;
        }

        @Nonnull
        @Override
        public String getName() {
            return resolvedKeySpacePath.getDirectory().getName();
        }

        @Nullable
        @Override
        public Object getLogicalValue() {
            return resolvedKeySpacePath.getLogicalValue();
        }

        @Nullable
        @Override
        public Object getResolvedValue() {
            return resolvedKeySpacePath.getResolvedValue();
        }
    }

    /**
     * A resolved record store keyspace, such as records or indexes.
     */
    public static class ResolvedRecordStoreKeyspace extends Resolved {
        @Nonnull
        private final FDBRecordStoreKeyspace recordStoreKeyspace;
        @Nullable
        private final RecordMetaData recordMetaData;
        @Nullable
        private final Object object;

        public ResolvedRecordStoreKeyspace(@Nonnull Resolved parent, @Nonnull FDBRecordStoreKeyspace recordStoreKeyspace,
                                           @Nullable RecordMetaData recordMetaData, @Nullable Object object) {
            super(parent);
            this.recordStoreKeyspace = recordStoreKeyspace;
            this.recordMetaData = recordMetaData;
            this.object = object;
        }

        @Nonnull
        public FDBRecordStoreKeyspace getRecordStoreKeyspace() {
            return recordStoreKeyspace;
        }

        @Nullable
        public RecordMetaData getRecordMetaData() {
            return recordMetaData;
        }

        @Nonnull
        @Override
        public String getName() {
            return recordStoreKeyspace.name();
        }

        @Nullable
        @Override
        public Object getLogicalValue() {
            return recordStoreKeyspace.key();
        }

        @Nullable
        @Override
        public Object getResolvedValue() {
            return object;
        }
    }

    /**
     * A resolved index keyspace.
     */
    public static class ResolvedIndexKeyspace extends Resolved {
        @Nonnull
        private final Index index;

        public ResolvedIndexKeyspace(@Nonnull Resolved parent, @Nonnull Index index) {
            super(parent);
            this.index = index;
        }

        @Nullable
        public Index getIndex() {
            return index;
        }

        @Nonnull
        @Override
        public String getName() {
            return "index";
        }

        @Nullable
        @Override
        public Object getLogicalValue() {
            return index.getName();
        }

        @Nullable
        @Override
        public Object getResolvedValue() {
            return index.getSubspaceKey();
        }
    }

    /**
     * A resolved record type key prefix keyspace.
     */
    public static class ResolvedRecordTypeKeyspace extends Resolved {
        @Nonnull
        private final RecordType recordType;

        public ResolvedRecordTypeKeyspace(@Nonnull Resolved parent, @Nonnull RecordType recordType) {
            super(parent);
            this.recordType = recordType;
        }

        @Nullable
        public RecordType getRecordType() {
            return recordType;
        }

        @Nonnull
        @Override
        public String getName() {
            return "record type";
        }

        @Nullable
        @Override
        public Object getLogicalValue() {
            return recordType.getName();
        }

        @Nullable
        @Override
        public Object getResolvedValue() {
            return recordType.getRecordTypeKey();
        }
    }

    /**
     * A resolved field of a record primary key or index key.
     */
    public static class ResolvedKeyField extends Resolved {
        @Nonnull
        private final String fieldName;
        @Nullable
        private final Object logicalValue;
        @Nullable
        private final Object resolvedValue;

        public ResolvedKeyField(@Nonnull Resolved parent, @Nonnull String fieldName, @Nullable Object logicalValue, @Nullable Object resolvedValue) {
            super(parent);
            this.fieldName = fieldName;
            this.logicalValue = logicalValue;
            this.resolvedValue = resolvedValue;
        }

        @Nonnull
        @Override
        public String getName() {
            return fieldName;
        }

        @Override
        @Nullable
        public Object getLogicalValue() {
            return logicalValue;
        }

        @Override
        @Nullable
        public Object getResolvedValue() {
            return resolvedValue;
        }
    }

    /**
     * Result of resolving a {@link Tuple} into a {@link Resolved} key space path or other
     * resolved element in the tree. This value is computed by taking a {@link Tuple} and
     * walking through the key space to a leaf node. Any remaining elements in the {@link Tuple}
     * become the "remainder".
     *
     * @see #resolveKeySpacePath(KeySpace, Tuple)
     */
    public static final class ResolvedAndRemainder {
        @Nonnull
        private final Resolved resolved;
        @Nonnull
        private final Tuple remainder;

        private ResolvedAndRemainder(@Nonnull Resolved resolved, @Nonnull Tuple remainder) {
            this.resolved = resolved;
            this.remainder = remainder;
        }

        /**
         * A {@link Resolved} key space path, etc. This represents some prefix of an original
         * {@link Tuple} that has been resolved to a value with known meaning. For instance,
         * a {@link ResolvedIndexKeyspace}, representing the index subspace of a record store.
         *
         * @return the portion of a {@link Tuple} that has been resolved to a known value
         */
        @Nonnull
        public Resolved getResolved() {
            return resolved;
        }

        /**
         * The suffix of a {@link Tuple} representing unmatched fields from the {@link KeySpace}.
         * This, for instance, may represent an individual index key whose elements do not have
         * semantic meaning.
         *
         * @return the unresovled suffix of some larger {@link Tuple}
         */
        @Nonnull
        public Tuple getRemainder() {
            return remainder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ResolvedAndRemainder that = (ResolvedAndRemainder)o;
            return Objects.equals(resolved, that.resolved) && Objects.equals(remainder, that.remainder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resolved, remainder);
        }

        @Override
        public String toString() {
            return resolved + "+" + remainder;
        }
    }

    public CompletableFuture<ResolvedAndRemainder> resolveKeySpacePath(@Nonnull KeySpace keySpace, @Nonnull Tuple tuple) {
        return resolvePathAndRemainder(new ResolvedRoot(keySpace), tuple);
    }

    public CompletableFuture<ResolvedAndRemainder> resolvePathAndRemainder(@Nonnull Resolved resolvedSubRoot, @Nonnull Tuple tuple) {
        AtomicReference<Resolved> current = new AtomicReference<>(resolvedSubRoot);
        AtomicInteger index = new AtomicInteger(0);
        return AsyncUtil.whileTrue(() -> {
            if (index.get() >= tuple.size()) {
                return AsyncUtil.READY_FALSE;
            }
            return resolve(current.get(), tuple.get(index.get())).thenApply(resolved -> {
                if (resolved == null) {
                    return false;
                }
                current.set(resolved);
                index.incrementAndGet();
                return true;
            });
        }).thenApply(vignore -> new ResolvedAndRemainder(current.get(), TupleHelpers.subTuple(tuple, index.get(), tuple.size())));
    }

    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Resolved> resolve(@Nonnull Resolved resolvedParent, @Nullable Object object) {
        if (resolvedParent.getDirectory() != null) {
            if (resolvedParent.getDirectory().getSubdirectories().isEmpty()) {
                if (isRecordStoreLeaf(resolvedParent, object)) {
                    // The top level keys in a record store use FDBRecordStoreKeyspace.
                    FDBRecordStoreKeyspace recordStoreKeyspace;
                    if (object == null) {
                        // The store header is read with a range scan from the first subkey (Tuple null) to detect various anomalies.
                        recordStoreKeyspace = FDBRecordStoreKeyspace.STORE_INFO;
                    } else {
                        try {
                            recordStoreKeyspace = FDBRecordStoreKeyspace.fromKey(object);
                        } catch (RecordCoreException ex) {
                            return UNRESOLVED;
                        }
                    }
                    if (recordStoreKeyspace != FDBRecordStoreKeyspace.STORE_INFO) {
                        return getRecordStoreMetaData(resolvedParent, object)
                                .thenApply(metaData -> new ResolvedRecordStoreKeyspace(resolvedParent, recordStoreKeyspace, metaData, object));
                    } else {
                        return CompletableFuture.completedFuture(new ResolvedRecordStoreKeyspace(resolvedParent, recordStoreKeyspace, null, object));
                    }
                }
            } else {
                final FDBRecordContext context = getContext();
                if (context == null) {
                    return UNRESOLVED;
                }
                try {
                    return resolvedParent.getDirectory().findChildForValue(context, resolvedParent.getResolvedPath(), object).handle((resolved, ex) -> {
                        // Null case includes swallowing async error (ex).
                        return resolved == null ? null : new ResolvedPath(resolvedParent, resolved);
                    });
                } catch (RecordCoreException ex) {
                    return UNRESOLVED;
                }
            }
        }
        return resolveNonDirectory(resolvedParent, object);
    }

    /**
     * Resolve something other than a {@link KeySpaceDirectory} node.
     * @param resolvedParent the resolved parent node
     * @param object the {@link com.apple.foundationdb.tuple.Tuple} element for this node
     * @return a future that completes to a new {@link Resolved} or {@code null}
     */
    protected CompletableFuture<Resolved> resolveNonDirectory(@Nonnull Resolved resolvedParent, @Nullable Object object) {
        int distance = 0;
        ResolvedRecordStoreKeyspace recordStoreKeyspace = null;
        ResolvedRecordTypeKeyspace recordTypeKeyspace = null;
        ResolvedIndexKeyspace indexKeyspace = null;
        for (Resolved resolved = resolvedParent; resolved != null && resolved.getDirectory() == null; resolved = resolved.getParent()) {
            if (resolved instanceof ResolvedRecordTypeKeyspace) {
                recordTypeKeyspace = (ResolvedRecordTypeKeyspace)resolved;
                break;
            }
            if (resolved instanceof ResolvedIndexKeyspace) {
                indexKeyspace = (ResolvedIndexKeyspace)resolved;
                break;
            }
            if (resolved instanceof ResolvedRecordStoreKeyspace) {
                recordStoreKeyspace = (ResolvedRecordStoreKeyspace)resolved;
                break;
            }
            distance++;
        }
        if (recordStoreKeyspace != null && recordStoreKeyspace.getRecordMetaData() != null) {
            switch (recordStoreKeyspace.getRecordStoreKeyspace()) {
                case RECORD:
                    if (distance == 0 && object != null && recordStoreKeyspace.getRecordMetaData().primaryKeyHasRecordTypePrefix()) {
                        final RecordType recordType;
                        try {
                            recordType = recordStoreKeyspace.getRecordMetaData().getRecordTypeFromRecordTypeKey(object);
                        } catch (RecordCoreException ex) {
                            break;
                        }
                        return CompletableFuture.completedFuture(new ResolvedRecordTypeKeyspace(resolvedParent, recordType));
                    }
                    KeyExpression commonPrimaryKey = recordStoreKeyspace.getRecordMetaData().commonPrimaryKey();
                    if (commonPrimaryKey != null) {
                        List<KeyExpression> storedPrimaryKeys = commonPrimaryKey.normalizeKeyForPositions();
                        if (distance < storedPrimaryKeys.size()) {
                            return resolvePrimaryKeyField(resolvedParent, object, storedPrimaryKeys.get(distance), distance);
                        }
                    }
                    break;
                case INDEX:
                case INDEX_SECONDARY_SPACE:
                case INDEX_RANGE_SPACE:
                case INDEX_UNIQUENESS_VIOLATIONS_SPACE:
                case INDEX_BUILD_SPACE:
                    // TODO: As of now, INDEX_STATE_SPACE has the index _name_, which doesn't really need resolving.
                    // Once https://github.com/FoundationDB/fdb-record-layer/issues/514 is addressed, that will need this, too.
                    if (distance == 0 && object != null) {
                        final Index index;
                        try {
                            index = recordStoreKeyspace.getRecordMetaData().getIndexFromSubspaceKey(object);
                        } catch (RecordCoreException ex) {
                            break;
                        }
                        return CompletableFuture.completedFuture(new ResolvedIndexKeyspace(resolvedParent, index));
                    }
                    break;
                default:
                    break;
            }
        }
        if (recordTypeKeyspace != null) {
            List<KeyExpression> storedPrimaryKeys = recordTypeKeyspace.getRecordType().getPrimaryKey().normalizeKeyForPositions();
            if (distance + 1 < storedPrimaryKeys.size()) {
                return resolvePrimaryKeyField(resolvedParent, object, storedPrimaryKeys.get(distance + 1), distance + 1);
            }
        }
        if (indexKeyspace != null &&
                indexKeyspace.getParent() instanceof ResolvedRecordStoreKeyspace &&
                ((ResolvedRecordStoreKeyspace)indexKeyspace.getParent()).getRecordStoreKeyspace() == FDBRecordStoreKeyspace.INDEX) {
            Index index = indexKeyspace.getIndex();
            List<KeyExpression> storedKeys = indexStoredKeys(index);
            if (distance < storedKeys.size()) {
                return resolveIndexField(resolvedParent, object, index, storedKeys.get(distance), distance);
            }
        }
        return UNRESOLVED;
    }

    // TODO: Get this from the IndexMaintainerFactory via some new interface (the IndexMaintainer needs a RecordStore).
    //  Also should include primary key fields when appropriate.
    protected List<KeyExpression> indexStoredKeys(@Nonnull Index index) {
        KeyExpression storedKey = index.getRootExpression();
        if (storedKey instanceof GroupingKeyExpression) {
            if (IndexTypes.RANK.equals(index.getType()) ||
                    IndexTypes.TIME_WINDOW_LEADERBOARD.equals(index.getType())) {
                // The grouped key(s) is also stored.
                storedKey = ((GroupingKeyExpression)storedKey).getWholeKey();
            } else {
                // The grouped key is reduced.
                storedKey = ((GroupingKeyExpression)storedKey).getGroupingSubKey();
            }
        }
        if (IndexTypes.TIME_WINDOW_LEADERBOARD.equals(index.getType())) {
            storedKey = Key.Expressions.concat(Key.Expressions.field("leaderboard"), storedKey);
        }
        return storedKey.normalizeKeyForPositions();
    }

    /**
     * Determine whether this leaf of the {@link KeySpaceDirectory} tree is the root of a {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore}.
     *
     * Override this if whether a leaf directory can be determined from the {@link Resolved}; for example, because they
     * use a specific {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePathWrapper} class.
     * @param resolvedParent the resolved parent node
     * @param object the {@link com.apple.foundationdb.tuple.Tuple} element for this node
     * @return {@code true} if this path stores a record store
     */
    public boolean isRecordStoreLeaf(@Nonnull Resolved resolvedParent, @Nullable Object object) {
        return false;
    }

    /**
     * Given a key space path for which {@link #isRecordStoreLeaf} is {@code true}, get the record store's meta-data.
     *
     * Override this method if the meta-data can be determined from the {@link Resolved} tree.
     * @param resolvedParent the resolved parent node
     * @param object the {@link com.apple.foundationdb.tuple.Tuple} element for this node
     * @return a future that completes to the record store's meta-data or {@code null}
     */
    protected CompletableFuture<RecordMetaData> getRecordStoreMetaData(@Nonnull Resolved resolvedParent, @Nullable Object object) {
        return NO_META_DATA;
    }

    protected CompletableFuture<Resolved> resolvePrimaryKeyField(@Nonnull Resolved resolvedParent, @Nullable Object object,
                                                                 @Nonnull KeyExpression fieldKey, int fieldIndex) {
        return resolveKeyField(resolvedParent, object, fieldKey);
    }

    protected CompletableFuture<Resolved> resolveIndexField(@Nonnull Resolved resolvedParent, @Nullable Object object,
                                                            @Nonnull Index index, @Nonnull KeyExpression fieldKey, int fieldIndex) {
        return resolveKeyField(resolvedParent, object, fieldKey);
    }

    protected CompletableFuture<Resolved> resolveKeyField(@Nonnull Resolved resolvedParent, @Nullable Object object,
                                                          @Nonnull KeyExpression fieldKey) {
        while (fieldKey instanceof NestingKeyExpression) {
            fieldKey = ((NestingKeyExpression)fieldKey).getChild();
        }
        if (fieldKey instanceof FieldKeyExpression) {
            return CompletableFuture.completedFuture(new ResolvedKeyField(resolvedParent, ((FieldKeyExpression)fieldKey).getFieldName(), object, object));
        }
        return UNRESOLVED;
    }
}

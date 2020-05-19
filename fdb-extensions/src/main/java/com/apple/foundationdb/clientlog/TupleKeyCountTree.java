/*
 * TupleKeyCountTree.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.clientlog;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A tree of occurrence counts tuple-encoded keys.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class TupleKeyCountTree {
    @Nonnull
    private final byte[] bytes;
    @Nullable
    private final Object object;

    private int count;
    @Nullable
    private final TupleKeyCountTree parent;
    @Nonnull
    private final Map<byte[], TupleKeyCountTree> children;

    private boolean visible = true;

    // Internal standin object for entries that don't actually have an object, just bytes that don't parse as a tuple.
    private static final Object UNPARSEABLE = new Object() {
        @Override
        public String toString() {
            return "*unparseable*";
        }
    };
    private static final Comparator<byte[]> BYTES_COMPARATOR = ByteArrayUtil::compareUnsigned;

    public TupleKeyCountTree() {
        this(null, new byte[0], null);
    }

    public TupleKeyCountTree(@Nullable TupleKeyCountTree parent, @Nonnull byte[] bytes, @Nullable Object object) {
        this.bytes = bytes;
        this.object = object;

        this.count = 0;
        this.parent = parent;
        this.children = new TreeMap<>(BYTES_COMPARATOR);
    }
    
    @Nonnull
    public byte[] getBytes() {
        return bytes;
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean hasObject() {
        return object != UNPARSEABLE;
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Object getObject() {
        if (object == UNPARSEABLE) {
            throw new IllegalStateException("node does not have a parseable object");
        }
        return object;
    }

    /**
     * Add the given tuple to the tree.
     *
     * Each element is added to the next deeper level in the tree, incrementing the count as it goes.
     * @param tuple the tuple to add
     */
    public void add(@Nonnull Tuple tuple) {
        addInternal(tuple.getItems(), 0, tuple.pack(), 0);
    }

    /**
     * Add encoded tuple bytes to the tree.
     *
     * @param packed the packed form of a tuple to be parsed and added to the tree
     */
    public void add(@Nonnull byte[] packed) {
        List<Object> items = null;
        int endPosition = packed.length;
        while (endPosition > 0) {
            try {
                items = Tuple.fromBytes(packed, 0, endPosition).getItems();
                break;
            } catch (IllegalArgumentException ex) {
                endPosition--;
            }
        }
        if (items == null) {
            items = new ArrayList<>();
        }
        if (endPosition < packed.length) {
            items.add(UNPARSEABLE);
        }
        addInternal(items, 0, packed, 0);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private synchronized void addInternal(@Nonnull List<Object> items, int itemPosition, @Nonnull byte[] bytes, int bytePosition) {
        count++;
        if (itemPosition < items.size()) {
            final Object childObject = items.get(itemPosition);
            final int byteSize = childObject == UNPARSEABLE ? bytes.length - bytePosition : Tuple.from(childObject).getPackedSize();
            final byte[] childBytes = Arrays.copyOfRange(bytes, bytePosition, bytePosition + byteSize);
            TupleKeyCountTree child = children.computeIfAbsent(childBytes, b -> newChild(b, childObject));
            child.addInternal(items, itemPosition + 1, bytes, bytePosition + byteSize);
        }
    }

    /**
     * Add a non-tuple object to the root of the tree.
     * @param prefix an object for the top level child of the tree
     * @return a subtree for the given object
     */
    @Nonnull
    public TupleKeyCountTree addPrefixChild(@Nonnull Object prefix) {
        count++;
        final byte[] prefixBytes = Tuple.from(prefix).pack();
        return children.computeIfAbsent(bytes, b -> newPrefixChild(prefixBytes, prefix));
    }

    @Nonnull
    protected TupleKeyCountTree newPrefixChild(@Nonnull byte[] bytes, @Nonnull Object prefix) {
        return newChild(bytes, prefix);
    }

    @Nonnull
    protected TupleKeyCountTree newChild(@Nonnull byte[] bytes, @Nonnull Object object) {
        return new TupleKeyCountTree(this, bytes, object);
    }

    public int getCount() {
        return count;
    }

    @Nullable
    public TupleKeyCountTree getParent() {
        return parent;
    }

    @Nonnull
    public Collection<TupleKeyCountTree> getChildren() {
        return children.values();
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    /**
     * Hide tree nodes that do not have counts at least as great as the given fraction of their parent node.
     * @param fraction the threshold count fraction
     */
    public void hideLessThanFraction(double fraction) {
        int threshold = (int)(count * fraction);
        for (TupleKeyCountTree child : children.values()) {
            child.setVisible(child.getCount() >= threshold);
            child.hideLessThanFraction(fraction);
        }
    }

    /**
     * Callback for {@link #printTree}.
     */
    @FunctionalInterface
    public interface Printer {
        void print(int depth, @Nonnull List<TupleKeyCountTree> path);
    }

    /**
     * Print this tree to the given printer.
     * @param printer the printer to be called for each visible level of the tree
     * @param collapseSeparator a string to be used to separate the printed forms of levels with only a single child
     */
    public void printTree(@Nonnull Printer printer, @Nullable String collapseSeparator) {
        if (parent != null) {
            printTree(0, 0, printer, collapseSeparator);
        } else {
            // Root does not print itself.
            for (TupleKeyCountTree child : children.values()) {
                child.printTree(0, 0, printer, collapseSeparator);
            }
        }
    }

    protected void printTree(int depth, int nancestors, @Nonnull Printer printer, @Nullable String collapseSeparator) {
        if (visible) {
            if (collapseSeparator != null) {
                TupleKeyCountTree onlyChild = null;
                for (TupleKeyCountTree child : children.values()) {
                    if (child.visible) {
                        if (onlyChild == null) {
                            onlyChild = child;
                        } else {
                            onlyChild = null;
                            break;
                        }
                    }
                }
                if (onlyChild != null) {
                    onlyChild.printTree(depth + 1, nancestors + 1, printer, collapseSeparator);
                    return;
                }
            }
            final List<TupleKeyCountTree> path;
            if (nancestors > 0) {
                path = new ArrayList<>(nancestors + 1);
                TupleKeyCountTree ancestor = this;
                for (int i = 0; i <= nancestors; i++) {
                    path.add(0, ancestor);
                    ancestor = ancestor.parent;
                }
            } else {
                path = Collections.singletonList(this);
            }
            printer.print(depth - nancestors, path);
            for (TupleKeyCountTree child : children.values()) {
                child.printTree(depth + 1, 0, printer, collapseSeparator);
            }
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public String toString() {
        if (object == UNPARSEABLE) {
            return ByteArrayUtil.printable(bytes);
        }
        if (object instanceof byte[]) {
            try {
                // Nested tuples are sometimes encoded as byte strings.
                Tuple tuple = Tuple.fromBytes((byte[])object);
                return tuple.toString();
            } catch (Exception ex) {
                return ByteArrayUtil.printable((byte[])object);
            }
        }
        return Objects.toString(object);
    }
}

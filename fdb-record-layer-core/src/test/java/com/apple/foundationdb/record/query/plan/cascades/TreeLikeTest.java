/*
 * TreeLikeTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TreeLike}.
 */
public class TreeLikeTest {

    @Test
    void testPreOrder1() {
        final TreeNode t = node("a", node("b"), node("c"));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPreOrder());
        assertEquals(ImmutableList.of("a", "b", "c"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c"),
                StreamSupport.stream(t.spliterator(), false).map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrder2() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPreOrder());
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e"),
                StreamSupport.stream(t.spliterator(), false).map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrder3() {
        final TreeNode t =
                node("a",
                        node("b",
                                node("c"),
                                node("d")),
                        node("e",
                                node("f",
                                    node("g"),
                                    node("h")),
                                node("i")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPreOrder());
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                StreamSupport.stream(t.spliterator(), false).map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrder4() {
        final TreeNode t =
                node("a",
                        node("b",
                                node("c")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPreOrder());
        assertEquals(ImmutableList.of("a", "b", "c"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c"),
                StreamSupport.stream(t.spliterator(), false).map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrderSingle() {
        final TreeNode t = node("a");

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPreOrder());
        assertEquals(ImmutableList.of("a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a"),
                StreamSupport.stream(t.spliterator(), false).map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPostOrder1() {
        final TreeNode t = node("a", node("b"), node("c"));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPostOrder());
        assertEquals(ImmutableList.of("b", "c", "a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
    }

    @Test
    void testPostOrder2() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPostOrder());
        assertEquals(ImmutableList.of("b", "d", "e", "c", "a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
    }

    @Test
    void testPostOrderSingle() {
        final TreeNode t = node("a");

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.inPostOrder());
        assertEquals(ImmutableList.of("a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
    }

    @Test
    void testFold1() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final int count =
                t.fold(n -> 1,
                        (c, foldedChildren) -> c + StreamSupport.stream(foldedChildren.spliterator(), false)
                                .mapToInt(childrenCount -> childrenCount)
                                .sum());
        assertEquals(5, count);
    }

    @Test
    void testFold2() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final String stringified =
                t.fold(TreeNode::getContents,
                        (n, foldedChildren) -> {
                            if (Iterables.isEmpty(foldedChildren)) {
                                return n;
                            } else {
                                return n + "(" +
                                       StreamSupport.stream(foldedChildren.spliterator(), false)
                                               .collect(Collectors.joining(", ")) + ")";
                            }
                        });
        assertEquals("a(b, c(d, e))", stringified);
    }

    @Test
    void testMap() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        // replace "c" with a "c'" with no children
        final Optional<TreeNode> mappedTreeOptional =
                t.mapMaybe((n, foldedChildren) -> {
                    final ImmutableList.Builder<TreeNode> builder = ImmutableList.builder();
                    final Iterator<? extends TreeNode> foldedChildrenIterator = foldedChildren.iterator();
                    for (final TreeNode child : n.getChildren()) {
                        if (!foldedChildrenIterator.hasNext()) {
                            return null;
                        }
                        if (child.getContents().equals("c")) {
                            builder.add(node("c'"));
                        } else {
                            builder.add(foldedChildrenIterator.next());
                        }
                    }
                    return new TreeNode(n.getContents(), builder.build());
                });

        assertTrue(mappedTreeOptional.isPresent());
        assertEquals(
                node("a",
                        node("b"),
                        node("c'")),
                mappedTreeOptional.get());
    }

    @Test
    void testMapWithNull() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final Optional<TreeNode> mappedTreeOptional =
                t.mapMaybe((n, foldedChildren) -> {
                    final ImmutableList.Builder<TreeNode> builder = ImmutableList.builder();
                    final Iterator<? extends TreeNode> foldedChildrenIterator = foldedChildren.iterator();
                    for (final TreeNode child : n.getChildren()) {
                        if (!foldedChildrenIterator.hasNext()) {
                            return null;
                        }
                        if (child.getContents().equals("c")) {
                            return null;
                        } else {
                            builder.add(foldedChildrenIterator.next());
                        }
                    }
                    return new TreeNode(n.getContents(), builder.build());
                });

        assertFalse(mappedTreeOptional.isPresent());
    }

    @Test
    void testMapLeaves() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final Optional<TreeNode> mappedTreeOptional =
                t.replaceLeavesMaybe(n -> {
                    if (n.getContents().equals("d")) {
                        return node("d'");
                    } else if (n.getContents().equals("e")) {
                        return node("e'");
                    } else {
                        return n;
                    }
                });
        assertTrue(mappedTreeOptional.isPresent());
        assertEquals(
                node("a",
                        node("b"),
                        node("c",
                                node("d'"),
                                node("e'"))),
                mappedTreeOptional.get());
    }

    private static class TreeNode implements TreeLike<TreeNode> {
        private final String contents;
        private final List<TreeNode> children;

        public TreeNode(@Nonnull final String contents, final Iterable<? extends TreeNode> children) {
            this.contents = contents;
            this.children = ImmutableList.copyOf(children);
        }

        @Nonnull
        public String getContents() {
            return contents;
        }

        @Nonnull
        @Override
        public TreeNode getThis() {
            return this;
        }

        @Nonnull
        @Override
        public Iterable<? extends TreeNode> getChildren() {
            return children;
        }

        @Nonnull
        @Override
        public TreeNode withChildren(@Nonnull final Iterable<? extends TreeNode> newChildren) {
            return new TreeNode(this.contents, newChildren);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TreeNode)) {
                return false;
            }
            final TreeNode treeNode = (TreeNode)o;
            return Objects.equal(getContents(), treeNode.getContents()) && Objects.equal(getChildren(), treeNode.getChildren());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getContents(), getChildren());
        }

        @Override
        public String toString() {
            return "(" + contents + "; " + getChildren() + ")";
        }
    }
    
    private static TreeNode node(@Nonnull final String contents, final TreeNode... nodes) {
        return new TreeNode(contents, ImmutableList.copyOf(nodes));
    }
}

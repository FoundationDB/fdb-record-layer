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
import com.google.common.collect.Iterators;
import org.junit.jupiter.api.Assertions;
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

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.preOrderIterable());
        assertEquals(ImmutableList.of("a", "b", "c"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c"),
                t.preOrderStream().map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrder2() {
        final TreeNode t =
                node("a",
                        node("b"),
                        node("c",
                                node("d"),
                                node("e")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.preOrderIterable());
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e"),
                t.preOrderStream().map(item -> item.contents).collect(Collectors.toList()));
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

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.preOrderIterable());
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                t.preOrderStream().map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrder4() {
        final TreeNode t =
                node("a",
                        node("b",
                                node("c")));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.preOrderIterable());
        assertEquals(ImmutableList.of("a", "b", "c"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a", "b", "c"),
                t.preOrderStream().map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPreOrderSingle() {
        final TreeNode t = node("a");

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.preOrderIterable());
        assertEquals(ImmutableList.of("a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
        assertEquals(ImmutableList.of("a"),
                t.preOrderStream().map(item -> item.contents).collect(Collectors.toList()));
    }

    @Test
    void testPostOrder1() {
        final TreeNode t = node("a", node("b"), node("c"));

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.postOrderIterable());
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

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.postOrderIterable());
        assertEquals(ImmutableList.of("b", "d", "e", "c", "a"),
                traversed.stream().map(TreeNode::getContents).collect(ImmutableList.toImmutableList()));
    }

    @Test
    void testPostOrderSingle() {
        final TreeNode t = node("a");

        final ImmutableList<? extends TreeNode> traversed = ImmutableList.copyOf(t.postOrderIterable());
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

    @Test
    void testReplace() {
        /*
         * replace can deform the tree, in this test, the tree is replaced with its leftmost root-to-leaf path by
         * doing the following replacement: n -> n.withLeftChild, due to the pre-order traversal nature, we get
         * the leftmost root-to-leaf path.
         *            a             ----->       a
         *        /      \                       |
         *       b        c         ----->       b
         *     /  \      / \                     |
         *    d   e     f   g       ----->       d
         */

        final TreeNode t =
                node("a",
                        node("b",
                                node("d"),
                                node("e")),
                        node("c",
                                node("f"),
                                node("g")));

        TreeNode mappedTreeOptional = t.replace(n -> new TreeNode(n.contents, n.children.isEmpty() ? n.children : ImmutableList.of(Iterables.get(n.children, 0))));
        assertEquals(
                node("a", node("b", node("d"))),
                mappedTreeOptional);

        /*
         * replace can deform the tree, in this test, the tree is replaced with its rightmost root-to-leaf path by
         * doing the following replacement: n -> n.withLeftChild, due to the pre-order traversal nature, we get
         * the rightmost root-to-leaf path.
         *            a             ----->       a
         *        /      \                       |
         *       b        c         ----->       c
         *     /  \      / \                     |
         *    d   e     f   g       ----->       g
         */
        mappedTreeOptional = t.replace(n -> new TreeNode(n.contents, n.children.isEmpty() ? n.children : ImmutableList.of(Iterables.get(n.children, Iterables.size(n.children) - 1))));
        assertEquals(
                node("a", node("c", node("g"))),
                mappedTreeOptional);

        /*
         * n -> node(Q) | n.contents = 'c'
         *            a             ----->                a
         *        /      \                            /      \
         *       b        c         ----->           b        Q
         *     /  \      / \                       /  \
         *    d    e    f   g       ----->        d    e
         */

        mappedTreeOptional = t.replace(n -> n.contents.equals("c") ? node("Q") : n);
        assertEquals(
                node("a",
                        node("b",
                                node("d"),
                                node("e")),
                        node("Q")),
                mappedTreeOptional);

        /*
         * n -> node(Q) | n.contents = 'c'
         * n -> node(S) | n.contents = 'b'
         *            a             ----->                a
         *        /      \                            /      \
         *       b        c         ----->           S        Q
         *     /  \      / \                       /  \
         *    d    e    f   g       ----->        d    e
         */

        mappedTreeOptional = t.replace(n -> {
            final var content = n.contents;
            if (content.equals("b")) {
                return new TreeNode("S", n.children);
            } else if (content.equals("c")) {
                return node("Q");
            } else {
                return n;
            }
        });
        assertEquals(
                node("a",
                        node("S",
                                node("d"),
                                node("e")),
                        node("Q")),
                mappedTreeOptional);
    }

    @Test
    void testReplaceCopyOnWrite() {
        /*
         * if the replacement predicate is a no-op, verify that the tree references are _not_ changed.
         *            a
         *        /      \
         *       b        c
         */

        TreeNode t =
                node("a",
                        node("b"),
                        node("c"));

        TreeNode replaced = t.replace(n -> n);

        assertEquals(
                node("a",
                        node("b"),
                        node("c")),
                replaced);

        Assertions.assertNotNull(replaced);
        Assertions.assertSame(replaced, t);
        Assertions.assertEquals(replaced.children, t.children);
        Assertions.assertSame(replaced.children, t.children);
        Assertions.assertSame(Iterators.get(replaced.children.iterator(), 0), Iterators.get(t.children.iterator(), 0));
        Assertions.assertSame(Iterators.get(replaced.children.iterator(), 1), Iterators.get(t.children.iterator(), 1));

        /*
         * changing a node, replaces the children of its parent with a new children list, but nothing else, and recursively
         * all ancestors.
         *              a              ----->            [a]
         *        /         \                       /          \
         *       b            c        ----->      [b            c]
         *     /   \       /     \               /   \       /     \
         *    d    e      f       g   ----->    d    e      [Q       g]
         */

        t = node("a",
                 node("b",
                         node("d"),
                         node("e")),
                 node("c",
                         node("f"),
                         node("g")));
        replaced = t.replace(n -> n.contents.equals("f") ? node("Q") : n);

        assertEquals(
                node("a",
                        node("b",
                                node("d"),
                                node("e")),
                        node("c",
                                node("Q"),
                                node("g"))),
                replaced);
        Assertions.assertNotNull(replaced);
        Assertions.assertNotSame(replaced, t); // "a" <--> "a"
        Assertions.assertNotSame(replaced.children, t.children); // "b", "c" <--> "b", "c"
        final var b = Iterators.get(replaced.children.iterator(), 0);
        final var otherB = Iterators.get(t.children.iterator(), 0);
        final var c = Iterators.get(replaced.children.iterator(), 1);
        final var otherC = Iterators.get(t.children.iterator(), 1);

        Assertions.assertSame(b, otherB);
        Assertions.assertSame(b.children, otherB.children); // "d", "e" <--> "d", "e"
        final var d = Iterators.get(b.children.iterator(), 0);
        final var otherD = Iterators.get(b.children.iterator(), 0);
        final var e = Iterators.get(otherB.children.iterator(), 1);
        final var otherE = Iterators.get(otherB.children.iterator(), 1);
        Assertions.assertSame(d, otherD);
        Assertions.assertSame(e, otherE);

        Assertions.assertNotSame(c, otherC);
        Assertions.assertNotSame(c.children, otherC.children); // "f", "g" <--> "Q", "g"
        final var f = Iterators.get(c.children.iterator(), 0);
        final var Q = Iterators.get(otherC.children.iterator(), 0);
        final var g = Iterators.get(c.children.iterator(), 1);
        final var otherG = Iterators.get(otherC.children.iterator(), 1);
        Assertions.assertNotSame(f, Q);
        Assertions.assertSame(g, otherG);
    }

    @Test
    void avoidInfiniteRecursionWhenReplacingLeaves() {
        /*
         * the stateless leaf replacement predicate wants to replace [c] with [d] -> [c] (where [c] appears _again_) leading to
         * infinite recursion
         * ideally, the consumer should care about avoiding infinite recursion,
         * nevertheless, TreeLike::replaceLeavesMaybe adds a protection mechanism against this, such that it guarantees
         * that newly-added leaves are _not_ passed to the replacement predicate.
         *            a                                 a
         *        /      \                 ->         /  \
         *       b        c                          b    d
         *                                                 \
         *                                                  c
         */
        TreeNode t =
                node("a",
                        node("b"),
                        node("c"));
        Optional<TreeNode> replacedMaybe = t.replaceLeavesMaybe(n -> {
            if (n.contents.equals("c")) {
                return node("d", node("c"));
            }
            return n;
        }, false);
        assertTrue(replacedMaybe.isPresent());
        assertEquals(
                node("a",
                        node("b"),
                        node("d",
                                node("c"))),
                replacedMaybe.get());
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

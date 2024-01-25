/*
 * PreOrderPerformance.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark;

import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.google.common.base.Objects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test performance of preorder implementations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 5000)
@SuppressWarnings("deprecation")
public class PreOrderPerformance {

    @Nonnull
    private static final Random random = new Random(42);

    @Nonnull
    private static TreeNode randomTree(int size, int maxLevelSize) {
        assert maxLevelSize <= size;
        assert size >= 1;

        final var nodeNames = IntStream.rangeClosed(1, size).mapToObj(String::valueOf).collect(Collectors.toList());
        TreeNode result = new TreeNode(nodeNames.get(0), List.of());
        var nodeCount = 0;
        Deque<Optional<TreeNode>> deque = new ArrayDeque<>();
        deque.add(Optional.of(result));
        deque.add(Optional.empty());
        while (nodeCount < size) {
            // read off all the nodes at this stage.
            if (deque.isEmpty()) {
                break;
            }
            final var item = deque.pop();
            if (deque.isEmpty()) {
                break;
            }
            if (item.isEmpty()) {
                deque.addLast(Optional.empty());
            } else {
                // add random children to current item
                final var finalNodeCount = nodeCount;
                final var children = IntStream.rangeClosed(1, 1 + random.nextInt(maxLevelSize))
                        .filter(i -> finalNodeCount + i < nodeNames.size())
                        .mapToObj(i -> new TreeNode(nodeNames.get(finalNodeCount + i), List.of()))
                        .collect(Collectors.toList());
                item.get().withChildren(children);
                deque.addAll(children.stream().map(Optional::of).collect(Collectors.toList()));
                nodeCount += children.size();
            }
        }
        return result;
    }

    private static final TreeNode tree = randomTree(100, 5);

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureNew() {
        final List<Iterable<? extends TreeNode>> result = new ArrayList<>(100000);
        for (int i = 0; i < 100000; ++i) {
            List<TreeNode> x = new ArrayList<>();
            tree.iterator().forEachRemaining(x::add);
            result.add(x);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureOld() {
        final List<Iterable<? extends TreeNode>> result = new ArrayList<>(1000000);
        for (int i = 0; i < 1000000; ++i) {
            final var x = tree.inPreOrder();
            result.add(x);
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(PreOrderPerformance.class.getSimpleName() )
                .addProfiler("gc")
                .build();

        new Runner(opt).run();
    }

    private static class TreeNode implements TreeLike<TreeNode> {
        private final String contents;
        private List<TreeNode> children;

        private final Supplier<Integer> heightSupplier;

        public TreeNode(@Nonnull final String contents, final Iterable<? extends TreeNode> children) {
            this.contents = contents;
            this.children = ImmutableList.copyOf(children);
            this.heightSupplier = Suppliers.memoize(this::computeHeight);
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
            children = ImmutableList.copyOf(newChildren);
            return this;
        }

        private int computeHeight() {
            return 1 + children.stream().mapToInt(TreeLike::height).max().orElse(0);
        }

        @Override
        public int height() {
            return heightSupplier.get();
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
}

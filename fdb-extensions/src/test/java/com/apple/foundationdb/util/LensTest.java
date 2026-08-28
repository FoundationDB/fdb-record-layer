/*
 * LensTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.util;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LensTest {
    @Test
    void testWrap() {
        final Lens<D, Integer> lens = D.xLens;
        assertThat(lens.wrap(6)).satisfies(
                d -> assertThat(d).isInstanceOf(D.class),
                d -> assertThat(d.x()).isEqualTo(6)
        );
    }

    @Test
    void testSet() {
        final A a = new A(new B(new D(10)), new C(null));
        final Lens<A, B> lens = A.bLens;
        assertThat(lens.set(a, new B(new D(20)))).satisfies(
                newA -> assertThat(newA).isInstanceOf(A.class),
                newA -> assertThat(newA.b().d().x()).isEqualTo(20)
        );
    }

    @Test
    void testGet() {
        final A a = new A(new B(new D(10)), new C(null));
        final Lens<A, B> lens = A.bLens;
        assertThat(lens.get(a)).satisfies(
                newB -> assertThat(newB).isInstanceOf(B.class),
                newB -> assertThat(newB.d().x()).isEqualTo(10)
        );
    }

    @Test
    void testSetGetWithNulls() {
        final A a = new A(new B(new D(10)), new C(null));
        final Lens<A, D> lens = A.cLens.compose(C.dLens);
        assertThatThrownBy(() -> lens.getNonnull(a)).isInstanceOf(NullPointerException.class);
        assertThat(lens.get(a)).isNull();
        final A nonNullCInA = lens.set(a, new D(100));
        assertThat(nonNullCInA).satisfies(
                newA -> assertThat(newA).isInstanceOf(A.class),
                newA -> assertThat(Objects.requireNonNull(newA.c().d()).x()).isEqualTo(100)
        );
        assertThat(lens.set(nonNullCInA, null)).satisfies(
                newA -> assertThat(newA).isInstanceOf(A.class),
                newA -> assertThat(newA.c().d()).isNull()
        );
    }

    @Test
    void testMap() {
        final A a = new A(new B(new D(10)), new C(null));
        final Lens<A, Integer> lens = A.bLens.compose(B.dLens).compose(D.xLens);
        assertThat(lens.map(a, old -> 2 * old))
                .satisfies(newA -> assertThat(newA.b().d().x()).isEqualTo(20));
    }

    @Test
    void testExtract() {
        final ImmutableList.Builder<B> bListBuilder = ImmutableList.builder();
        for (int i = 0; i < 10; i ++) {
            bListBuilder.add(new B(new D(i)));
        }
        final List<B> bList = bListBuilder.build();

        final Lens<B, Integer> lens = B.dLens.compose(D.xLens);
        assertThat(Lens.extract(lens, bList)).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void testCompose() {
        final A a = new A(new B(new D(10)), new C(null));
        final Lens<A, Integer> lens = A.bLens.compose(B.dLens).compose(D.xLens);
        assertThat(lens.get(a)).isEqualTo(10);
        assertThat(lens.set(a, 20)).satisfies(newA -> assertThat(newA.b().d().x()).isEqualTo(20));
    }

    private record A(B b, C c) {
        private static final Lens<A, B> bLens = new Lens<>() {
            @Override
            public B get(final A a) {
                return a.b();
            }

            @Override
            public A set(@Nullable final A a, @Nullable final B b) {
                return new A(Objects.requireNonNull(b), Objects.requireNonNull(a).c());
            }
        };

        private static final Lens<A, C> cLens = new Lens<>() {
            @Override
            public C get(final A a) {
                return a.c();
            }

            @Override
            public A set(@Nullable final A a, @Nullable final C c) {
                return new A(Objects.requireNonNull(a).b(), Objects.requireNonNull(c));
            }
        };
    }

    private record B(D d) {
        private static final Lens<B, D> dLens = new Lens<>() {
            @Override
            public D get(final B b) {
                return b.d();
            }

            @Override
            public B set(@Nullable final B b, @Nullable final D d) {
                return new B(Objects.requireNonNull(d));
            }
        };
    }

    private record C(@Nullable D d) {
        private static final Lens<C, D> dLens = new Lens<>() {
            @Nullable
            @Override
            public D get(final C c) {
                return c.d();
            }

            @Override
            public C set(@Nullable final C c, @Nullable final D d) {
                return new C(d);
            }
        };
    }

    private record D(int x) {
        private static final Lens<D, Integer> xLens = new Lens<>() {
            @Override
            public Integer get(final D d) {
                return d.x();
            }

            @Override
            public D set(@Nullable final D d, @Nullable final Integer x) {
                return new D(Objects.requireNonNull(x));
            }
        };
    }
}

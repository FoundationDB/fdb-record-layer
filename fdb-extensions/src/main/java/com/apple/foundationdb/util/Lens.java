/*
 * Lens.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * A typed "focus" on a part {@code A} sitting inside a whole {@code C} — the object-oriented
 * shape of the well-known functional <i>lens</i>. A {@code Lens<C, A>} carries two operations:
 * a {@link #get(Object) get} that extracts the inner {@code A} from a {@code C}, and a
 * {@link #set(Object, Object) set} that produces an updated {@code C} (or a fresh one) with a
 * new inner {@code A}.
 *
 * <p>Where this useful is when generic library code needs to operate on the {@code A}
 * <em>inside</em> caller-supplied {@code C}s without forcing the caller to unwrap everything
 * upfront and re-wrap afterward. The caller passes a lens that knows how to peek at and rebuild
 * their domain object; the library stays oblivious to {@code C} beyond the lens.
 *
 * <h2>Example</h2>
 * Suppose the caller has labeled vectors and wants to hand them to a generic clustering API that
 * only cares about the geometric vector inside each one:
 * <pre>{@code
 * record Labeled(String name, RealVector vector) {}
 *
 * Lens<Labeled, RealVector> vectorLens = new Lens<>() {
 *     public RealVector get(Labeled c) {
 *         return c.vector();
 *     }
 *     public Labeled set(Labeled c, RealVector v) {
 *         // c == null signals "no existing container, build a fresh one from a"
 *         return new Labeled(c == null ? "" : c.name(), v);
 *     }
 * };
 *
 * // Library code can now generically work over List<Labeled> without ever importing Labeled:
 * RealVector v        = pointLens.getNonnull(record);                     // peek
 * Labeled  updated    = pointLens.set(record, normalized);                // replace
 * Labeled  wrapped    = pointLens.wrap(newVector);                        // build fresh
 * Labeled  scaledByTwo = pointLens.map(record, p -> p.multiply(2.0d));    // read-modify-write
 * }</pre>
 *
 * <h2>Composition</h2>
 * Lenses compose: {@link #compose(Lens)} chains a downstream lens so the result focuses through
 * two layers at once. For example, {@code outerLens.compose(innerLens)} produces a lens that
 * reaches the inner {@code A2} of an inner {@code A} inside an outer {@code C}.
 * {@link #identity()} returns the trivial lens whose container and part are the same type — useful
 * as a default argument when the caller's input <em>is</em> the attribute.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #get(Object) get} may return {@code null} if the implementation models an
 *       absent attribute. Use {@link #getNonnull(Object) getNonnull} as a convenience when the
 *       caller is sure the attribute is present.</li>
 *   <li>{@link #set(Object, Object) set} accepts a nullable {@code C}: {@code set(null, a)} is the
 *       "construct a new container around {@code a}" case, surfaced as the more readable
 *       {@link #wrap(Object) wrap}.</li>
 *   <li>Implementations are expected to be pure functions of their inputs (no shared mutable
 *       state). Whether {@code set} returns a fresh container or mutates {@code c} is up to the
 *       implementation; callers should always use the returned value rather than assume identity
 *       with the argument.</li>
 * </ul>
 *
 * @param <C> the container ("whole") type
 * @param <A> the attribute ("part") type focused by this lens
 */
public interface Lens<C, A> {
    /**
     * Like {@link #get(Object)} but throws {@link NullPointerException} if the attribute is
     * missing. Use when the caller is certain the attribute is present and wants the
     * non-null type to flow through downstream code.
     *
     * @param c the container; must not be {@code null}
     * @return the extracted attribute, never {@code null}
     * @throws NullPointerException if {@code get(c)} returns {@code null}
     */
    @Nonnull
    default A getNonnull(@Nonnull final C c) {
        return Objects.requireNonNull(get(c));
    }

    /**
     * Extracts the focused attribute from {@code c}.
     *
     * @param c the container; must not be {@code null}
     * @return the attribute, or {@code null} if the implementation models an absent attribute
     */
    @Nullable
    A get(@Nonnull C c);

    /**
     * Builds a fresh container around {@code a} — equivalent to {@code set(null, a)} but reads
     * more naturally at the call site when there is no existing container to update.
     *
     * @param a the attribute to wrap; may be {@code null} if the implementation supports
     *          containers without the attribute
     * @return a non-null container holding {@code a}
     */
    @Nonnull
    default C wrap(@Nullable final A a) {
        return set(null, a);
    }

    /**
     * Returns a container equivalent to {@code c} but with the focused attribute set to {@code a}.
     * Passing {@code null} for {@code c} signals "no existing container, build a fresh one around
     * {@code a}" — implementations are expected to handle this case, typically with a default
     * for the unfocused parts of the container.
     *
     * @param c the existing container to update, or {@code null} to construct a fresh one
     * @param a the new value of the focused attribute; may be {@code null} if the implementation
     *          supports containers without the attribute
     * @return a non-null container with the attribute set to {@code a}
     */
    @Nonnull
    C set(@Nullable C c, @Nullable A a);

    /**
     * Read-modify-write: extracts the attribute, applies {@code operator}, and returns a
     * container with the result. Reference-equality short-circuits: if {@code operator} returns
     * the same instance it received, {@code c} itself is returned with no call to
     * {@link #set(Object, Object) set}.
     *
     * @param c the container; must not be {@code null}
     * @param operator transformation applied to the extracted attribute
     * @return a container reflecting the transformed attribute, possibly {@code c} itself
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    default C map(@Nonnull final C c, @Nonnull final UnaryOperator<A> operator) {
        final A oldA = get(c);
        final A newA = operator.apply(oldA);
        if (newA == oldA) {
            return c;
        }
        return set(c, newA);
    }

    /**
     * Returns a lens that focuses through this lens and then through {@code downstream}, so the
     * resulting lens reaches the inner attribute {@code A2} of an inner {@code A} inside a
     * {@code C}. The composed lens's {@link #set(Object, Object) set} implements the standard
     * lens-composition law: get the intermediate {@code A}, apply {@code downstream.set} to
     * embed {@code a2} into it, then write the new {@code A} back into {@code c}.
     *
     * @param downstream the inner lens that focuses from the intermediate {@code A} onto {@code A2}
     * @param <A2> the deeply focused attribute type
     * @return a composed lens from {@code C} to {@code A2}
     */
    default <A2> Lens<C, A2> compose(@Nonnull final Lens<A, A2> downstream) {
        return new Lens<>() {
            @Nullable
            @Override
            public A2 get(@Nonnull final C c) {
                return downstream.get(Lens.this.get(c));
            }

            @Nonnull
            @Override
            @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
            public C set(@Nullable final C c, @Nullable final A2 a2) {
                return Lens.this.set(c, downstream.set(Lens.this.get(c), a2));
            }
        };
    }

    /**
     * The identity lens: container and attribute are the same type and the lens is a no-op pair
     * of {@code get(t) -> t} and {@code set(t, t2) -> t2}. Handy as a default argument when the
     * caller's input <em>is</em> already the attribute type, or to seed a {@link #compose chain}.
     *
     * @param <T> shared container/attribute type
     * @return a lens that focuses each element on itself
     */
    @Nonnull
    static <T> Lens<T, T> identity() {
        return new Lens<>() {
            @Nonnull
            @Override
            public T get(@Nonnull final T t) {
                return t;
            }

            @Nonnull
            @Override
            @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
            public T set(@Nullable final T t, @Nullable final T t2) {
                return Objects.requireNonNull(t2);
            }
        };
    }

    /**
     * Convenience for the common batch-extract pattern: applies {@link #getNonnull(Object)} to every element of
     * {@code elements} and collects the results into a fresh immutable list. Equivalent to a streams pipeline but
     * skips the boilerplate and preserves order.
     *
     * @param lens the lens to apply to each element
     * @param elements the source list
     * @param <T1> the source ("container") element type
     * @param <T2> the extracted ("attribute") type
     * @return an immutable list of extracted attributes in source order; never {@code null}
     * @throws NullPointerException if any element extraction yields {@code null}
     */
    @Nonnull
    static <T1, T2> List<T2> extract(@Nonnull final Lens<T1, T2> lens, @Nonnull final List<T1> elements) {
        final ImmutableList.Builder<T2> resultsBuilder = ImmutableList.builder();
        for (final T1 element : elements) {
            resultsBuilder.add(lens.getNonnull(element));
        }
        return resultsBuilder.build();
    }
}

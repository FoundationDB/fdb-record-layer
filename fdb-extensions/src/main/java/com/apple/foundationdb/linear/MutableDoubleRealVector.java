/*
 * MutableDoubleRealVector.java
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

package com.apple.foundationdb.linear;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * Mutable double-precision vector. Extends {@link DoubleRealVector} with a set of explicit
 * in-place mutation methods suffixed {@code *This} ({@link #addToThis(RealVector)},
 * {@link #subtractFromThis(RealVector)}, {@link #multiplyThisBy(double)},
 * {@link #normalizeThis()}, {@link #zeroThis()}, {@link #setData(double[])}). Each writes through
 * the receiver's existing storage and returns {@code this}, so they compose into chains without
 * allocating.
 *
 * <p>The non-{@code *This} arithmetic methods inherited from {@link DoubleRealVector}
 * ({@code add}, {@code subtract}, {@code multiply}, {@code normalize}, {@code withData}) are
 * deliberately <em>not</em> overridden here — they continue to allocate fresh
 * {@link DoubleRealVector} results and leave the receiver untouched. This split keeps the choice
 * of mutation visible at every call site: {@code v.add(w)} always produces a new value;
 * {@code v.addToThis(w)} always mutates {@code v}. A caller can therefore use the same
 * {@code MutableDoubleRealVector} reference under both idioms without surprise.
 *
 * <p>Typical use is short-lived hot loops that reuse a single storage buffer — most prominently
 * the centroid update in k-means, which {@link #zeroThis() zeros} and then accumulates assigned
 * vectors into the same instance once per Lloyd iteration. For values that must remain stable
 * after construction, hand out {@link #toImmutable()} (which copies into a fresh immutable
 * {@link DoubleRealVector}) rather than the mutable reference itself.
 *
 * <p>Implementation notes worth knowing:
 * <ul>
 *   <li>The parent class memoizes the hash code and the converted representations
 *       ({@link DoubleRealVector#toHalfRealVector()}, {@link DoubleRealVector#toFloatRealVector()},
 *       {@link AbstractRealVector#getRawData()}). Mutation would invalidate those caches, so this
 *       class overrides each to recompute on every call.</li>
 *   <li>{@link #toMutable()} returns {@code this} (no copy) since the receiver is already mutable.
 *       Callers who need an independent buffer must explicitly construct a new instance over
 *       {@code getData().clone()}.</li>
 *   <li>{@link #toDoubleRealVector()} also returns {@code this}, typed as
 *       {@code MutableDoubleRealVector}. Code that captures the result under a
 *       {@code DoubleRealVector} reference is still pointing at this mutable buffer; subsequent
 *       mutations will be visible through that reference.</li>
 *   <li>Underlying storage is the same {@code double[]} held by
 *       {@link AbstractRealVector#data}; the {@code double[]} constructor stores the array
 *       <em>by reference</em>, and {@link #getData()} returns the live array. Mutation methods
 *       write directly through it, so any external alias of that array sees the changes.</li>
 * </ul>
 */
@SuppressWarnings("PMD.OverrideBothEqualsAndHashcode")
public class MutableDoubleRealVector extends DoubleRealVector {

    /**
     * Constructs a mutable vector from a boxed {@code Double[]}. The input is defensively copied
     * into a fresh primitive array, so subsequent changes to {@code doubleData} do not affect the
     * returned vector.
     *
     * @param doubleData the components of the new vector
     */
    public MutableDoubleRealVector(@Nonnull final Double[] doubleData) {
        this(computeDoubleData(doubleData));
    }

    /**
     * Constructs a mutable vector over the given primitive array. The array is stored
     * <em>by reference</em> — the new vector reads and writes through it, and any external alias
     * of {@code data} will see every subsequent mutation. Use {@code data.clone()} at the call
     * site if you need an independent backing store.
     *
     * @param data the components of the new vector; ownership is shared with the caller per the
     *             aliasing contract above
     */
    public MutableDoubleRealVector(@Nonnull final double[] data) {
        super(data);
    }

    /**
     * Constructs a mutable vector by widening each int component to a {@code double}. The new
     * vector owns its backing array — the input is read but not retained.
     *
     * @param intData the components of the new vector
     */
    public MutableDoubleRealVector(@Nonnull final int[] intData) {
        this(fromInts(intData));
    }

    /**
     * Constructs a mutable vector by widening each long component to a {@code double}. The new
     * vector owns its backing array — the input is read but not retained.
     *
     * @param longData the components of the new vector
     */
    public MutableDoubleRealVector(@Nonnull final long[] longData) {
        this(fromLongs(longData));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Override note: the parent memoizes the half-precision projection because immutable
     * vectors never change. This class's contents can change, so memoization would return stale
     * values — every call recomputes from the current data.
     */
    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return computeHalfRealVector();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Override note: same reasoning as {@link #toHalfRealVector()} — recomputed on every call
     * because the underlying data may have changed since the last invocation.
     */
    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return computeFloatRealVector();
    }

    /**
     * Returns {@code this}, typed as {@code MutableDoubleRealVector}. No copy is made; the result
     * shares storage with the receiver, so any subsequent mutation is visible through both
     * references.
     */
    @Nonnull
    @Override
    public MutableDoubleRealVector toDoubleRealVector() {
        return this;
    }

    /**
     * Returns {@code this} — the receiver is already mutable, so no copy is made. Callers that
     * need an independent mutable buffer should explicitly construct one over
     * {@code getData().clone()}.
     */
    @Nonnull
    @Override
    public MutableDoubleRealVector toMutable() {
        return this;
    }

    /**
     * Returns a snapshot of the current contents as a fresh immutable
     * {@link DoubleRealVector}. The underlying {@code double[]} is cloned, so subsequent
     * mutations of the receiver do not affect the returned value.
     */
    @Nonnull
    @Override
    public DoubleRealVector toImmutable() {
        return new DoubleRealVector(getData().clone());
    }

    /**
     * Replaces this vector's components with those of {@code data} (element-wise copy through the
     * existing backing array). The receiver's storage is reused — no allocation — and {@code data}
     * is read but not retained.
     *
     * @param data the new component values; must have the same length as this vector
     * @return {@code this} for chaining
     * @throws IllegalArgumentException if {@code data.length} does not match this vector's
     *         dimensionality
     */
    @Nonnull
    public MutableDoubleRealVector setData(@Nonnull final double[] data) {
        Preconditions.checkArgument(this.data.length == data.length);
        System.arraycopy(data, 0, this.data, 0, this.data.length);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Override note: the parent's memoized raw-bytes representation would be stale once any
     * mutation happens; this override recomputes on every call.
     */
    @Nonnull
    @Override
    public byte[] getRawData() {
        return computeRawData();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Override note: hash codes are intentionally <em>not</em> memoized for mutable vectors —
     * mutation changes equality and therefore the hash. Treat mutable instances as unsuitable
     * keys for hash-based collections while they might still be mutated; snapshot via
     * {@link #toImmutable()} before keying.
     */
    @Override
    public int hashCode() {
        return computeHashCode();
    }

    /**
     * Normalizes this vector to unit L2 norm in place.
     *
     * @return {@code this} for chaining
     * @throws IllegalArgumentException if this vector's L2 norm is zero, infinite, or NaN
     */
    @Nonnull
    public MutableDoubleRealVector normalizeThis() {
        RealVectorPrimitives.normalizeInto(this, getData());
        return this;
    }

    /**
     * Adds {@code other} to this vector component-wise, in place.
     *
     * @param other the vector to add; must have the same dimensionality as this vector
     * @return {@code this} for chaining
     */
    @Nonnull
    public MutableDoubleRealVector addToThis(@Nonnull final RealVector other) {
        RealVectorPrimitives.addInto(this, other, getData());
        return this;
    }

    /**
     * Adds {@code scalar} to every component of this vector, in place.
     *
     * @param scalar the value to add to each component
     * @return {@code this} for chaining
     */
    @Nonnull
    public MutableDoubleRealVector addToThis(final double scalar) {
        RealVectorPrimitives.addInto(this, scalar, getData());
        return this;
    }

    /**
     * Subtracts {@code other} from this vector component-wise, in place.
     *
     * @param other the vector to subtract; must have the same dimensionality as this vector
     * @return {@code this} for chaining
     */
    @Nonnull
    public MutableDoubleRealVector subtractFromThis(@Nonnull final RealVector other) {
        RealVectorPrimitives.subtractInto(this, other, getData());
        return this;
    }

    /**
     * Subtracts {@code scalar} from every component of this vector, in place.
     *
     * @param scalar the value to subtract from each component
     * @return {@code this} for chaining
     */
    @Nonnull
    public MutableDoubleRealVector subtractFromThis(final double scalar) {
        RealVectorPrimitives.subtractInto(this, scalar, getData());
        return this;
    }

    /**
     * Multiplies every component of this vector by {@code scalar}, in place.
     *
     * @param scalar the factor to scale each component by
     * @return {@code this} for chaining
     */
    @Nonnull
    public MutableDoubleRealVector multiplyThisBy(final double scalar) {
        RealVectorPrimitives.multiplyInto(this, scalar, getData());
        return this;
    }

    /**
     * Sets every component of this vector to zero, in place. The dimensionality is unchanged.
     *
     * @return {@code this} for chaining; the return is annotated {@link CanIgnoreReturnValue}
     *         because callers commonly reset state without consuming the result
     */
    @CanIgnoreReturnValue
    @Nonnull
    public MutableDoubleRealVector zeroThis() {
        Arrays.fill(getData(), 0.0d);
        return this;
    }

    /**
     * Returns a new mutable vector of the given dimensionality with all components set to zero.
     *
     * @param numDimensions number of dimensions; must be non-negative
     * @return a freshly allocated zero vector
     */
    @Nonnull
    public static MutableDoubleRealVector zeroVector(final int numDimensions) {
        return new MutableDoubleRealVector(new double[numDimensions]);
    }

    /**
     * Creates a {@link MutableDoubleRealVector} from the serialized form produced by
     * {@link DoubleRealVector#getRawData()}.
     * <p>
     * The input is interpreted as a leading type byte followed by a sequence of 64-bit big-endian
     * IEEE-754 doubles; each 8-byte run becomes one component of the resulting vector. The
     * returned instance owns a fresh backing array; the byte array is read but not retained.
     *
     * @param vectorBytes the serialized form
     * @return a new mutable vector with the decoded components
     */
    @Nonnull
    public static MutableDoubleRealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        return new MutableDoubleRealVector(DoubleRealVector.decodeDoubleBytes(vectorBytes));
    }
}

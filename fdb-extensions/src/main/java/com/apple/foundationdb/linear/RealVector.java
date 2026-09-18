/*
 * RealVector.java
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

import com.apple.foundationdb.half.Half;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * Real-valued mathematical vector — the common API every dense vector representation in this
 * package implements. Concrete subtypes differ only in element precision and storage layout:
 * {@link DoubleRealVector} stores 64-bit doubles, {@link FloatRealVector} stores 32-bit floats
 * truncated to doubles, {@link HalfRealVector} stores 16-bit halves likewise. All numerical
 * methods on this interface return values in {@code double} regardless of the underlying
 * precision; the precision difference shows up as round-trip loss in
 * {@link #toHalfRealVector()} / {@link #toFloatRealVector()} conversions, not in arithmetic.
 *
 * <p>The interface exposes three families of operations:
 * <ul>
 *   <li><b>Shape and access</b> — {@link #getNumDimensions()}, {@link #getComponent(int)},
 *       {@link #getData()}.</li>
 *   <li><b>Arithmetic</b> — {@link #add(RealVector) add}, {@link #subtract(RealVector) subtract},
 *       {@link #multiply(double) multiply}, {@link #dot(RealVector) dot},
 *       {@link #normalize() normalize}, plus the squared-/raw-norm pair
 *       {@link #l2SquaredNorm()} and {@link #l2Norm()} and the cheaper
 *       {@link #l2SquaredDistance(RealVector)} pairwise distance. Default implementations
 *       allocate a fresh result vector and never mutate the receiver; the mutating variants live
 *       on {@link MutableDoubleRealVector}.</li>
 *   <li><b>Conversion and serialization</b> — precision conversions
 *       ({@link #toHalfRealVector()}, {@link #toFloatRealVector()},
 *       {@link #toDoubleRealVector()}), mutability swaps
 *       ({@link #toMutable()}, {@link #toImmutable()}), and the
 *       byte-array round-trip ({@link #getRawData()} / {@link #fromBytes(byte[])}).</li>
 * </ul>
 */
public interface RealVector {
    /**
     * Threshold (in L2-norm units) below which a vector is treated as "effectively zero" — i.e.
     * its direction is considered undefined for metrics that depend on it, principally cosine
     * similarity. Used by {@link #isNearlyZeroNorm()}, which compares the squared L2 norm
     * against {@code EPS * EPS} to avoid a {@code sqrt}.
     * <p>
     * The value is sized to sit well above the floating-point noise of typical double-precision
     * accumulations and well below any norm a meaningful vector would have in practice. Callers
     * generally shouldn't need to consult this constant directly; prefer {@link #isNearlyZeroNorm()}.
     */
    double EPS = 1.0e-12;

    /**
     * Cached snapshot of {@link VectorType#values()} as an immutable list. Used by
     * {@link #fromVectorTypeOrdinal(int)} so the type lookup avoids re-cloning the enum's
     * value array on every call.
     */
    ImmutableList<VectorType> VECTOR_TYPES = ImmutableList.copyOf(VectorType.values());

    /**
     * Returns the number of elements in the vector, i.e. the number of dimensions.
     * @return the number of dimensions
     */
    int getNumDimensions();

    /**
     * Gets the component of this object at the specified dimension.
     * <p>
     * The dimension is a zero-based index. For a 3D vector, for example, dimension 0 might correspond to the
     * x-component, 1 to the y-component, and 2 to the z-component. This method provides direct access to the
     * underlying data element.
     * @param dimension the zero-based index of the component to retrieve.
     * @return the component at the specified dimension, which is guaranteed to be non-null.
     * @throws IndexOutOfBoundsException if the {@code dimension} is negative or
     *         greater than or equal to the number of dimensions of this object.
     */
    double getComponent(int dimension);

    /**
     * Returns the underlying data array.
     * <p>
     * The returned array is guaranteed to be non-null. Note that this method
     * returns a direct reference to the internal array, not a copy.
     * @return the data array of type {@code R[]}, never {@code null}.
     */
    @Nonnull
    double[] getData();

    /**
     * Returns a new vector of the same precision and length as the receiver but with the given
     * component data. Implementations decide whether the returned vector aliases {@code data}
     * (immutable subtypes typically do; mutable subtypes copy through their existing storage).
     *
     * @param data the components for the new vector; length must match this vector's
     *        dimensionality
     * @return a non-null vector with the given data
     */
    @Nonnull
    RealVector withData(@Nonnull double[] data);

    /**
     * Gets the raw byte data representation of this object.
     * <p>
     * This method provides a direct, unprocessed view of the object's underlying data. The format of the byte array is
     * implementation-specific and should be documented by the concrete class that implements this method.
     * @return a non-null byte array containing the raw data.
     */
    @Nonnull
    byte[] getRawData();

    /**
     * Converts this object into a {@code RealVector} of {@link Half} precision floating-point numbers.
     * <p>
     * As this is an abstract method, implementing classes are responsible for defining the specific conversion logic
     * from their internal representation to a {@code RealVector} using {@link Half} objects to serialize and
     * deserialize the vector. If this object already is a {@code HalfRealVector} this method should return {@code this}.
     * @return a non-null {@link HalfRealVector} containing the {@link Half} precision floating-point representation of
     *         this object.
     */
    @Nonnull
    HalfRealVector toHalfRealVector();

    /**
     * Converts this object into a {@code RealVector} of single precision floating-point numbers.
     * <p>
     * As this is an abstract method, implementing classes are responsible for defining the specific conversion logic
     * from their internal representation to a {@code RealVector} using floating point numbers to serialize and
     * deserialize the vector. If this object already is a {@code FloatRealVector} this method should return
     * {@code this}.
     * @return a non-null {@link FloatRealVector} containing the single precision floating-point representation of
     *         this object.
     */
    @Nonnull
    FloatRealVector toFloatRealVector();

    /**
     * Converts this vector into a {@link DoubleRealVector}.
     * <p>
     * This method provides a way to obtain a double-precision floating-point representation of the vector. If the
     * vector is already an instance of {@code DoubleRealVector}, this method may return the instance itself. Otherwise,
     * it will create a new {@code DoubleRealVector} containing the same elements, which may involve a conversion of the
     * underlying data type.
     * @return a non-null {@link DoubleRealVector} representation of this vector.
     */
    @Nonnull
    DoubleRealVector toDoubleRealVector();

    /**
     * Returns a {@link MutableDoubleRealVector} carrying the same components as this vector.
     * The default implementation clones the underlying data so the returned mutable instance is
     * independent of the receiver; {@link MutableDoubleRealVector#toMutable()} overrides this
     * to return {@code this} since it's already mutable.
     *
     * @return a fresh (or in the case of {@link MutableDoubleRealVector}, the same) mutable
     *         double-precision vector
     */
    @Nonnull
    default MutableDoubleRealVector toMutable() {
        return new MutableDoubleRealVector(getData().clone());
    }

    /**
     * Returns an immutable view of this vector — i.e. one whose components cannot be subsequently
     * mutated through any reference. Immutable subtypes return {@code this};
     * {@link MutableDoubleRealVector#toImmutable()} returns a fresh
     * {@link DoubleRealVector} with cloned data.
     *
     * @return a non-null immutable vector with the same components as this vector
     */
    @Nonnull
    RealVector toImmutable();

    /**
     * Returns the dot product {@code Σ_i this[i] * other[i]}. The receiver is not mutated.
     *
     * @param other the right operand; must have the same dimensionality as this vector
     * @return the dot product
     * @throws IllegalArgumentException if {@code other} has a different dimensionality
     */
    default double dot(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        return RealVectorPrimitives.dot(getData(), other.getData());
    }

    /**
     * Returns the squared Euclidean distance to {@code other}, i.e. {@code Σ_i (this[i] - other[i])^2}.
     * Equivalent to but cheaper than {@code subtract(other).l2SquaredNorm()} (no temporary
     * allocation), and cheaper than {@code Math.pow(estimator.distance(this, other), 2)} for the
     * Euclidean metric (skips a {@code sqrt} that would just be squared again).
     */
    default double l2SquaredDistance(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        return RealVectorPrimitives.euclideanSquared(getData(), other.getData());
    }

    /**
     * Returns {@code true} when this vector's L2 norm is at or below the {@link #EPS} threshold,
     * i.e. when the vector is "effectively zero" and its direction is undefined for cosine-style
     * metrics. Implemented as a squared-norm comparison so no {@code sqrt} is needed.
     *
     * @return {@code true} if this vector is effectively zero
     */
    default boolean isNearlyZeroNorm() {
        return l2SquaredNorm() <= EPS * EPS;
    }

    /**
     * Returns the L2 (Euclidean) norm {@code sqrt(Σ_i this[i]^2)}. Prefer
     * {@link #l2SquaredNorm()} when you only need to compare or threshold magnitudes — it skips
     * the {@code sqrt}.
     *
     * @return the L2 norm of this vector
     */
    default double l2Norm() {
        return Math.sqrt(l2SquaredNorm());
    }

    /**
     * Returns the squared L2 norm {@code Σ_i this[i]^2}. The default routes through
     * {@link RealVectorPrimitives#l2SquaredNorm(double[])} so the active SIMD/scalar backend is
     * picked transparently; subtypes are encouraged to override with a memoized variant, since
     * the value is reused by {@link #l2Norm()} and several distance helpers.
     *
     * @return the squared L2 norm of this vector
     */
    default double l2SquaredNorm() {
        return RealVectorPrimitives.l2SquaredNorm(getData());
    }

    /**
     * Returns a new vector pointing in the same direction as this vector but scaled to unit L2
     * norm. The receiver is not mutated; the result is a fresh allocation of the same precision
     * type.
     *
     * @return a non-null unit-norm vector
     * @throws IllegalArgumentException if this vector's L2 norm is zero, infinite, or NaN —
     *         direction is undefined in those cases
     */
    @Nonnull
    default RealVector normalize() {
        return withData(RealVectorPrimitives.normalizeInto(this.getData(), new double[getNumDimensions()]));
    }

    /**
     * Bit-reproducible counterpart to {@link #dot(RealVector)} that forces the scalar backend.
     * Prefer this over {@link #dot(RealVector)} whenever the result is persisted and later
     * compared byte-for-byte across machines: the ambient SIMD backend can differ from scalar
     * (and between SIMD hosts of different vector widths) by a few ULPs, which is enough to flip
     * a downstream hash or {@code floor} boundary. It costs a scalar reduction and is otherwise
     * identical to {@link #dot(RealVector)}.
     *
     * @param other the right operand; must have the same dimensionality as this vector
     * @return the dot product, computed on the scalar backend
     * @throws IllegalArgumentException if {@code other} has a different dimensionality
     */
    default double dotExact(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        return RealVectorPrimitives.dotExact(getData(), other.getData());
    }

    /**
     * Bit-reproducible counterpart to {@link #l2SquaredNorm()} that forces the scalar backend.
     * Unlike {@link #l2SquaredNorm()}, this is intentionally <em>not</em> memoized: it always
     * recomputes from {@link #getData()} so it can never return a value that an earlier SIMD call
     * cached on a memoizing subtype. See {@link #dotExact(RealVector)} for when to prefer the
     * exact variants.
     *
     * @return the squared L2 norm, computed on the scalar backend
     */
    default double l2SquaredNormExact() {
        return RealVectorPrimitives.l2SquaredNormExact(getData());
    }

    /**
     * Bit-reproducible counterpart to {@link #l2Norm()} that forces the scalar backend, via
     * {@link #l2SquaredNormExact()}. See {@link #dotExact(RealVector)}.
     *
     * @return the L2 norm, computed on the scalar backend
     */
    default double l2NormExact() {
        return Math.sqrt(l2SquaredNormExact());
    }

    /**
     * Bit-reproducible counterpart to {@link #l2SquaredDistance(RealVector)} that forces the
     * scalar backend. See {@link #dotExact(RealVector)}.
     *
     * @param other the right operand; must have the same dimensionality as this vector
     * @return the squared Euclidean distance, computed on the scalar backend
     * @throws IllegalArgumentException if {@code other} has a different dimensionality
     */
    default double l2SquaredDistanceExact(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        return RealVectorPrimitives.euclideanSquaredExact(getData(), other.getData());
    }

    /**
     * Bit-reproducible counterpart to {@link #normalize()} that forces the scalar backend for the
     * underlying norm. Only the norm is a reduction and thus backend-sensitive; the subsequent
     * element-wise scaling is bit-identical on either backend. See {@link #dotExact(RealVector)}.
     *
     * @return a non-null unit-norm vector, normalized using the scalar backend
     * @throws IllegalArgumentException if this vector's L2 norm is zero, infinite, or NaN
     */
    @Nonnull
    default RealVector normalizeExact() {
        return withData(RealVectorPrimitives.normalizeIntoExact(this.getData(), new double[getNumDimensions()]));
    }

    /**
     * Returns a new vector whose components are the element-wise sum of this vector and
     * {@code other}. The receiver is not mutated.
     *
     * @param other the right operand; must have the same dimensionality as this vector
     * @return a non-null vector with {@code result[i] = this[i] + other[i]}
     * @throws IllegalArgumentException if {@code other} has a different dimensionality
     */
    @Nonnull
    default RealVector add(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.addInto(this.getData(), other.getData(), new double[getNumDimensions()]));
    }

    /**
     * Returns a new vector with {@code scalar} added to every component of this vector. The
     * receiver is not mutated.
     *
     * @param scalar the value to add to each component
     * @return a non-null vector with {@code result[i] = this[i] + scalar}
     */
    @Nonnull
    default RealVector add(final double scalar) {
        return withData(RealVectorPrimitives.addInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns a new vector whose components are the element-wise difference of this vector and
     * {@code other}. The receiver is not mutated.
     *
     * @param other the right operand; must have the same dimensionality as this vector
     * @return a non-null vector with {@code result[i] = this[i] - other[i]}
     * @throws IllegalArgumentException if {@code other} has a different dimensionality
     */
    @Nonnull
    default RealVector subtract(@Nonnull final RealVector other) {
        return withData(RealVectorPrimitives.subtractInto(this.getData(), other.getData(), new double[getNumDimensions()]));
    }

    /**
     * Returns a new vector with {@code scalar} subtracted from every component of this vector.
     * The receiver is not mutated.
     *
     * @param scalar the value to subtract from each component
     * @return a non-null vector with {@code result[i] = this[i] - scalar}
     */
    @Nonnull
    default RealVector subtract(final double scalar) {
        return withData(RealVectorPrimitives.subtractInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns a new vector with every component of this vector scaled by {@code scalar}. The
     * receiver is not mutated.
     *
     * @param scalar the factor to scale each component by
     * @return a non-null vector with {@code result[i] = this[i] * scalar}
     */
    @Nonnull
    default RealVector multiply(final double scalar) {
        return withData(RealVectorPrimitives.multiplyInto(this.getData(), scalar, new double[getNumDimensions()]));
    }

    /**
     * Returns the {@link VectorType} with the given ordinal in
     * {@link VectorType#values() VectorType.values()}, looked up from the cached
     * {@link #VECTOR_TYPES} list. Used while deserializing a vector to dispatch to the right
     * subtype's {@code fromBytes}.
     *
     * @param ordinal the type's enum ordinal
     * @return the matching {@link VectorType}; never {@code null}
     * @throws IndexOutOfBoundsException if {@code ordinal} is not a valid enum ordinal
     */
    @Nonnull
    static VectorType fromVectorTypeOrdinal(final int ordinal) {
        return VECTOR_TYPES.get(ordinal);
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This method interprets the input byte array by interpreting the first byte of the array as the type of vector.
     * It then delegates to {@link #fromBytes(VectorType, byte[])} to do the actual deserialization.
     *
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    static RealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        final byte vectorTypeOrdinal = vectorBytes[0];
        return fromBytes(fromVectorTypeOrdinal(vectorTypeOrdinal), vectorBytes);
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This implementation dispatches to the actual logic that deserialize a byte array to a vector which is located in
     * the respective implementations of {@link RealVector}.
     * @param vectorType the vector type of the serialized vector
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    static RealVector fromBytes(@Nonnull final VectorType vectorType, @Nonnull final byte[] vectorBytes) {
        return switch (vectorType) {
            case HALF -> HalfRealVector.fromBytes(vectorBytes);
            case SINGLE -> FloatRealVector.fromBytes(vectorBytes);
            case DOUBLE -> DoubleRealVector.fromBytes(vectorBytes);
            default -> throw new RuntimeException("unable to deserialize vector");
        };
    }
}

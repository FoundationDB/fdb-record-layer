/*
 * EncodedRealVector.java
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

package com.apple.foundationdb.rabitq;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.VectorType;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Wire/storage representation of a RaBitQ-quantized vector.
 *
 * <p>Each component is encoded into {@code numExBits + 1} bits (one sign bit plus
 * {@code numExBits} magnitude "extra bits", following the RaBitQ paper's terminology), tightly
 * bit-packed for storage. Three per-vector calibration constants — {@link #fAddEx},
 * {@link #fRescaleEx}, {@link #fErrorEx} — accompany the integer codes; they are produced by
 * {@link RaBitQuantizer} during encoding and consumed by {@link RaBitDistanceEstimator} during
 * distance evaluation. The encoded form is dramatically smaller than the equivalent
 * {@link DoubleRealVector} (roughly {@code (numExBits + 1) / 64} of the bytes) while still
 * supporting fast distance estimation against other RaBitQ-encoded queries.
 *
 * <p>This class implements {@link RealVector} so it composes with the rest of the linear-algebra
 * surface, but it is fundamentally an opaque encoded blob, not a dense numeric vector. Two
 * consequences of that worth knowing:
 * <ul>
 *   <li>{@link #getData()} lazily <em>reconstructs</em> an approximate dense
 *       {@code double[]} from the codes plus calibration constants (see {@link #computeData()}).
 *       This is a best-effort dequantization, not the original vector — round-tripping through
 *       encoded form is lossy by design.</li>
 *   <li>{@link #withData(double[])} returns a fresh {@link DoubleRealVector}, not another
 *       {@code EncodedRealVector}, because re-encoding requires the quantizer's full per-call
 *       state (rotation seed, calibration sweep). As a result, the arithmetic methods inherited
 *       from {@link RealVector} ({@code add}, {@code subtract}, {@code multiply},
 *       {@code normalize}) produce ordinary double vectors and discard the encoding.</li>
 * </ul>
 *
 * <p>The wire format produced by {@link #getRawData()} is a leading {@link VectorType#RABITQ}
 * type byte, the three calibration doubles in big-endian order, then the bit-packed integer
 * codes. {@link #fromBytes} parses the format back, given the dimensionality and
 * {@code numExBits} (which are not stored in the byte array — the caller is expected to know
 * them from surrounding context, typically the quantizer's configuration).
 */
@SuppressWarnings("checkstyle:MemberName")
public class EncodedRealVector implements RealVector {
    /**
     * Number of magnitude bits per dimension, in addition to the implicit sign bit. The total
     * bit budget per component is {@code numExBits + 1}.
     */
    private final int numExBits;

    /**
     * Per-dimension integer codes. Each entry is the signed code for one component, occupying
     * {@code numExBits + 1} bits when packed into the wire format. Length equals
     * {@link #getNumDimensions()}.
     */
    @Nonnull
    private final int[] encoded;

    /**
     * Additive term used during distance estimation against this vector. For Euclidean and the
     * inner-product metrics this is {@code ||residual||²}.
     */
    private final double fAddEx;

    /**
     * Multiplicative rescale used during distance estimation against this vector. For Euclidean
     * and the inner-product metrics this is {@code (1/ip) * (-2 * ||residual||)} where
     * {@code ip} is the inner product the quantizer chose to normalize against.
     */
    private final double fRescaleEx;

    /**
     * Estimated upper bound on the per-vector encoding error, in the metric's native scale.
     * Used by {@link #computeData()} to size the confidence weight when reconstructing the
     * approximate dense form.
     */
    private final double fErrorEx;

    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<double[]> dataSupplier = Suppliers.memoize(this::computeData);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<byte[]> rawDataSupplier = Suppliers.memoize(this::computeRawData);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<HalfRealVector> toHalfRealVectorSupplier = Suppliers.memoize(this::computeHalfRealVector);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<FloatRealVector> toFloatRealVectorSupplier = Suppliers.memoize(this::computeFloatRealVector);
    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Double> l2SquaredNormSupplier = Suppliers.memoize(this::computeL2SquaredNorm);

    /**
     * Constructs an encoded vector from the raw outputs of {@link RaBitQuantizer}. The
     * {@code encoded} array is stored by reference (no defensive copy); callers must not mutate
     * it after handing it over.
     *
     * @param numExBits number of magnitude bits per component (not counting the sign bit)
     * @param encoded per-dimension integer codes; ownership transfers to this vector
     * @param fAddEx the additive term ({@link #getAddEx()})
     * @param fRescaleEx the multiplicative rescale ({@link #getRescaleEx()})
     * @param fErrorEx the per-vector error bound ({@link #getErrorEx()})
     */
    public EncodedRealVector(final int numExBits, @Nonnull final int[] encoded, final double fAddEx, final double fRescaleEx,
                             final double fErrorEx) {
        this.numExBits = numExBits;
        this.encoded = encoded;
        this.fAddEx = fAddEx;
        this.fRescaleEx = fRescaleEx;
        this.fErrorEx = fErrorEx;
    }

    /**
     * Returns the underlying integer-code array (no copy). Each entry is the signed code for
     * one component, packed into {@code numExBits + 1} bits when serialized. Callers must not
     * mutate the returned array.
     *
     * @return the per-dimension code array
     */
    @Nonnull
    public int[] getEncodedData() {
        return encoded;
    }

    /**
     * Returns the per-vector additive term used during distance estimation. See {@link #fAddEx}.
     */
    public double getAddEx() {
        return fAddEx;
    }

    /**
     * Returns the per-vector multiplicative rescale used during distance estimation.
     * See {@link #fRescaleEx}.
     */
    public double getRescaleEx() {
        return fRescaleEx;
    }

    /**
     * Returns the per-vector error bound used during distance estimation and dequantization.
     * See {@link #fErrorEx}.
     */
    public double getErrorEx() {
        return fErrorEx;
    }

    /**
     * Two encoded vectors compare equal iff they have identical code arrays and identical
     * calibration constants ({@link #fAddEx}, {@link #fRescaleEx}, {@link #fErrorEx}). The
     * dequantized representation is intentionally not consulted — equality is on the encoding,
     * not on the (lossy) reconstruction.
     */
    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof EncodedRealVector)) {
            return false;
        }

        final EncodedRealVector that = (EncodedRealVector)o;
        return Double.compare(fAddEx, that.fAddEx) == 0 &&
                Double.compare(fRescaleEx, that.fRescaleEx) == 0 &&
                Double.compare(fErrorEx, that.fErrorEx) == 0 &&
                Arrays.equals(encoded, that.encoded);
    }

    /**
     * Returns the memoized hash code, computed from the code array and the three calibration
     * doubles — consistent with {@link #equals(Object)}.
     */
    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    /**
     * Computes the hash from scratch, backing the memoizing {@link #hashCodeSupplier}.
     *
     * @return the hash code
     */
    public int computeHashCode() {
        int result = Arrays.hashCode(encoded);
        result = 31 * result + Double.hashCode(fAddEx);
        result = 31 * result + Double.hashCode(fRescaleEx);
        result = 31 * result + Double.hashCode(fErrorEx);
        return result;
    }

    @Override
    public int getNumDimensions() {
        return encoded.length;
    }

    /**
     * Returns the raw integer code for the component at the given dimension. Unlike
     * {@link #getComponent(int)} this does not trigger dequantization — useful for distance
     * estimators that operate directly on the encoded representation.
     *
     * @param dimension the zero-based dimension index
     * @return the integer code for that dimension
     * @throws IndexOutOfBoundsException if {@code dimension} is out of range
     */
    public int getEncodedComponent(final int dimension) {
        return encoded[dimension];
    }

    /**
     * {@inheritDoc}
     *
     * <p>Reads from the lazily reconstructed dense form (see {@link #getData()}); the first
     * call materializes the full {@code double[]} via {@link #computeData()}. If you only need
     * the raw integer code, prefer {@link #getEncodedComponent(int)} which skips the
     * reconstruction.
     */
    @Override
    public double getComponent(final int dimension) {
        return getData()[dimension];
    }

    /**
     * Returns the lazily reconstructed dense form of this encoded vector. The reconstruction
     * is approximate (see {@link #computeData()} for details) and memoized so repeated calls
     * are cheap.
     */
    @Nonnull
    @Override
    public double[] getData() {
        return dataSupplier.get();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns a fresh {@link DoubleRealVector} carrying {@code data} — <em>not</em> a
     * re-encoded {@code EncodedRealVector}, because re-encoding requires the quantizer's
     * per-call state (rotation seed, calibration sweep). This means the inherited arithmetic
     * methods ({@code add}, {@code subtract}, {@code multiply}, {@code normalize}) all drop
     * back to ordinary double-precision results.
     */
    @Nonnull
    @Override
    public RealVector withData(@Nonnull final double[] data) {
        // we explicitly make this a normal double vector instead of an encoded vector
        return new DoubleRealVector(data);
    }

    /**
     * Reconstructs an approximate dense {@code double[]} from the encoded codes plus
     * {@link #fAddEx}. Backs the memoizing supplier behind {@link #getData()}; callers should
     * normally go through that path.
     *
     * <p>The reconstruction is a <em>norm-matched</em> dequantization. The codes carry the
     * sign-and-magnitude pattern of the original residual {@code r} but at the integer scale of
     * the encoder; this method centers them by subtracting {@code cb = 2^numExBits - 0.5} and
     * rescales the result so its L2 norm equals {@code ||r||}, which is recoverable as
     * {@code √fAddEx}. Concretely, with {@code z = encoded - cb}:
     * <pre>{@code
     *   output[i] = z[i] · (||r|| / ||z||)
     * }</pre>
     * The returned vector therefore has direction equal to {@code z}'s direction (an
     * approximation of {@code r}'s direction, modulo quantization error) and magnitude exactly
     * equal to the original residual's norm. This is the right reconstruction whenever the
     * caller wants to treat the encoded vector "like a normal {@link RealVector}" — distances
     * against non-encoded vectors come out at the original scale, k-means input vectors mix
     * cleanly with non-encoded ones, etc.
     *
     * <p>Note that the per-vector estimator constants {@link #fRescaleEx} and {@link #fErrorEx}
     * are intentionally <em>not</em> used here. Those drive the
     * {@link RaBitDistanceEstimator}'s internal distance formula, where the scaling and
     * confidence shrinkage live; pulling them into the dequantization would produce a
     * confidence-weighted "view" suitable for reversing the estimator's math but not for
     * treating the result as a stand-in for the original residual.
     *
     * <p>Degenerate inputs: if {@link #fAddEx} is non-positive (the original residual was
     * effectively zero) or if every {@code encoded[i] = cb} (the centered codes have zero
     * norm), there's no meaningful direction to recover and a zero vector is returned. No
     * exception is thrown for these cases.
     *
     * @return a freshly allocated {@code double[]} with the reconstructed components; never
     *         {@code null}
     */
    @Nonnull
    double[] computeData() {
        final int numDimensions = getNumDimensions();
        final double cb = (1 << numExBits) - 0.5;
        final double[] xucData = new double[numDimensions];
        double xucNormSqr = 0.0;
        for (int i = 0; i < xucData.length; i++) {
            final double component = encoded[i] - cb;
            xucData[i] = component;
            xucNormSqr += component * component;
        }

        // Degenerate case: the original vector had zero norm, so the best reconstruction is the zero vector.
        if (!(fAddEx > 0.0) || xucNormSqr == 0.0) {
            return new double[numDimensions];
        }

        // Reconstruct the signed quantized direction and scale it back to the stored vector norm.
        // fAddEx stores ||v||^2, while encoded - cb stores the signed code direction used by the estimator.
        final double scale = Math.sqrt(fAddEx / xucNormSqr);
        for (int i = 0; i < xucData.length; i++) {
            xucData[i] *= scale;
        }
        return xucData;
    }

    /**
     * Returns the memoized wire-format serialization of this vector (see
     * {@link #computeRawData()} for the format).
     */
    @Nonnull
    @Override
    public byte[] getRawData() {
        return rawDataSupplier.get();
    }

    /**
     * Serializes this encoded vector into a byte array in the RaBitQ wire format. Layout (all
     * multi-byte values big-endian):
     * <ol>
     *   <li>1 byte — {@link VectorType#RABITQ} ordinal as a type tag.</li>
     *   <li>8 bytes — {@link #fAddEx}.</li>
     *   <li>8 bytes — {@link #fRescaleEx}.</li>
     *   <li>8 bytes — {@link #fErrorEx}.</li>
     *   <li>{@code ceil(numDimensions * (numExBits + 1) / 8)} bytes — the per-dimension
     *       integer codes, tightly bit-packed by {@link #packEncodedComponents(int, ByteBuffer)}.</li>
     * </ol>
     * Note that the dimensionality and {@code numExBits} are not stored in the byte stream;
     * {@link #fromBytes(byte[], int, int)} requires both as explicit arguments.
     *
     * @return the serialized form; never {@code null}
     */
    @Nonnull
    protected byte[] computeRawData() {
        int numBits = getNumDimensions() * (numExBits + 1); // congruency with paper
        final int length = 25 +        // RABITQ (byte) + fAddEx (double) + fRescaleEx (double) + fErrorEx (double)
                (numBits - 1) / 8 + 1; // snap byte array to the smallest length fitting all bits
        final byte[] vectorBytes = new byte[length];
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte)VectorType.RABITQ.ordinal());
        buffer.putDouble(fAddEx);
        buffer.putDouble(fRescaleEx);
        buffer.putDouble(fErrorEx);
        packEncodedComponents(numExBits, buffer);
        return vectorBytes;
    }

    /**
     * Bit-packs the integer codes into {@code buffer}, writing {@code numExBits + 1} bits per
     * component in big-endian (MSB-first) order. Components that span byte boundaries are split
     * so high-order bits go into the current byte and low-order bits flow into the next. After
     * the last component, any partially filled trailing byte is flushed.
     *
     * @param numExBits number of magnitude bits per component (sign bit added implicitly)
     * @param buffer the destination, positioned where packed bits should start being written
     */
    private void packEncodedComponents(final int numExBits, @Nonnull final ByteBuffer buffer) {
        // big-endian
        final int bitsPerComponent = numExBits + 1; // congruency with paper
        int remainingBitsInByte = 8;
        byte currentByte = 0;
        for (int i = 0; i < getNumDimensions(); i++) {
            final int component = getEncodedComponent(i);
            int remainingBitsInComponent = bitsPerComponent;

            while (remainingBitsInComponent > 0) {
                final int remainingMask = (1 << remainingBitsInComponent) - 1;
                final int remainingComponent = component & remainingMask;

                if (remainingBitsInComponent <= remainingBitsInByte) {
                    currentByte = (byte)(currentByte | (remainingComponent << (remainingBitsInByte - remainingBitsInComponent)));
                    remainingBitsInByte -= remainingBitsInComponent;
                    if (remainingBitsInByte == 0) {
                        remainingBitsInByte = 8;
                        buffer.put(currentByte);
                        currentByte = 0;
                    }
                    break;
                }

                // remainingBitsInComponent > bitOffset
                currentByte = (byte)(currentByte | (remainingComponent >> (remainingBitsInComponent - remainingBitsInByte)));
                remainingBitsInComponent -= remainingBitsInByte;
                remainingBitsInByte = 8;
                buffer.put(currentByte);
                currentByte = 0;
            }
        }

        if (remainingBitsInByte < 8) {
            buffer.put(currentByte);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Computed from the lazily reconstructed dense form (see {@link #getData()}); the
     * resulting {@link HalfRealVector} is memoized.
     */
    @Nonnull
    @Override
    public HalfRealVector toHalfRealVector() {
        return toHalfRealVectorSupplier.get();
    }

    /**
     * Builds a fresh half-precision dense vector from the reconstructed components. Used as
     * the supplier behind {@link #toHalfRealVector()}.
     */
    @Nonnull
    private HalfRealVector computeHalfRealVector() {
        return new HalfRealVector(getData());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Computed from the lazily reconstructed dense form (see {@link #getData()}); the
     * resulting {@link FloatRealVector} is memoized.
     */
    @Nonnull
    @Override
    public FloatRealVector toFloatRealVector() {
        return toFloatRealVectorSupplier.get();
    }

    /**
     * Builds a fresh single-precision dense vector from the reconstructed components. Used as
     * the supplier behind {@link #toFloatRealVector()}.
     */
    @Nonnull
    private FloatRealVector computeFloatRealVector() {
        return new FloatRealVector(getData());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns a fresh {@link DoubleRealVector} carrying the reconstructed dense components.
     * Unlike the half/float conversions, this one is not memoized — the underlying
     * {@code double[]} reconstruction is already memoized by {@link #getData()}, so wrapping
     * it again is cheap.
     */
    @Nonnull
    @Override
    public DoubleRealVector toDoubleRealVector() {
        return new DoubleRealVector(getData());
    }

    /**
     * Returns {@code this} — instances of this class are already immutable.
     */
    @Nonnull
    @Override
    public EncodedRealVector toImmutable() {
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Computed from the reconstructed dense form (it's {@code dot(this)} on the dequantized
     * components) and memoized.
     */
    @Override
    public double l2SquaredNorm() {
        return l2SquaredNormSupplier.get();
    }

    /**
     * Computes the squared L2 norm from scratch as {@code dot(this)}. Backs the memoizing
     * supplier behind {@link #l2SquaredNorm()}.
     *
     * @return the squared L2 norm of the reconstructed dense form
     */
    private double computeL2SquaredNorm() {
        return dot(this);
    }

    /**
     * Deserializes an encoded vector from the wire format produced by {@link #computeRawData()}.
     * The dimensionality and {@code numExBits} are not stored in the byte stream and must be
     * supplied by the caller (typically from the encoder's configuration).
     *
     * @param vectorBytes the serialized form; must start with the {@link VectorType#RABITQ}
     *        type tag
     * @param numDimensions number of components in the encoded vector
     * @param numExBits number of magnitude bits per component (sign bit implicit)
     * @return a freshly allocated encoded vector
     * @throws com.google.common.base.VerifyException if the leading type tag is not
     *         {@link VectorType#RABITQ}
     */
    @Nonnull
    public static EncodedRealVector fromBytes(@Nonnull final byte[] vectorBytes,
                                              final int numDimensions,
                                              final int numExBits) {
        final ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(ByteOrder.BIG_ENDIAN);
        Verify.verify(buffer.get() == VectorType.RABITQ.ordinal());

        final double fAddEx = buffer.getDouble();
        final double fRescaleEx = buffer.getDouble();
        final double fErrorEx = buffer.getDouble();
        final int[] components = unpackComponents(buffer, numDimensions, numExBits);
        return new EncodedRealVector(numExBits, components, fAddEx, fRescaleEx, fErrorEx);
    }

    /**
     * Inverse of {@link #packEncodedComponents(int, ByteBuffer)}: reads {@code numExBits + 1}
     * bits per component out of {@code buffer} in big-endian (MSB-first) order and returns the
     * decoded integer codes.
     *
     * @param buffer the source, positioned at the start of the packed bits
     * @param numDimensions number of components to decode
     * @param numExBits number of magnitude bits per component (sign bit implicit)
     * @return a freshly allocated array of decoded codes
     */
    @Nonnull
    private static int[] unpackComponents(@Nonnull final ByteBuffer buffer,
                                          final int numDimensions,
                                          final int numExBits) {
        int[] result = new int[numDimensions];

        // big-endian
        final int bitsPerComponent = numExBits + 1; // congruency with paper
        int remainingBitsInByte = 8;
        byte currentByte = buffer.get();
        for (int i = 0; i < numDimensions; i++) {
            int remainingBitsForComponent = bitsPerComponent;

            while (remainingBitsForComponent > 0) {
                final int mask = (1 << remainingBitsInByte) - 1;
                int maskedByte = currentByte & mask;

                if (remainingBitsForComponent <= remainingBitsInByte) {
                    result[i] |= maskedByte >> (remainingBitsInByte - remainingBitsForComponent);

                    remainingBitsInByte -= remainingBitsForComponent;
                    if (remainingBitsInByte == 0) {
                        remainingBitsInByte = 8;
                        currentByte = (i + 1 == numDimensions) ? 0 : buffer.get();
                    }
                    break;
                }

                // remainingBitsForComponent > remainingBitsInByte
                result[i] |= maskedByte << remainingBitsForComponent - remainingBitsInByte;
                remainingBitsForComponent -= remainingBitsInByte;
                remainingBitsInByte = 8;
                currentByte = buffer.get();
            }
        }
        return result;
    }
}

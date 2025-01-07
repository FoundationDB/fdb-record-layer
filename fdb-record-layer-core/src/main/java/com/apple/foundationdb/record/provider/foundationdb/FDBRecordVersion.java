/*
 * FDBRecordVersion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Class representing a specific version within FDB. This
 * is designed to inter-operate with the {@link Versionstamp} class used to serialize
 * versions within FoundationDB.
 *
 * The version consists of two parts: a global version and a local version. The global version
 * is a {@value GLOBAL_VERSION_LENGTH} byte version that is usually set by the
 * database, and it should be used to impose an ordering between different transactions. The
 * local version should be set by the user instead, and it should be used to impose an order
 * on different records within a single transaction. Together, this two versions are
 * combined into a {@value VERSION_LENGTH} byte array that can be used to impose a total order
 * across all records.
 */
@API(API.Status.UNSTABLE)
public class FDBRecordVersion implements Comparable<FDBRecordVersion> {

    private static final byte[] INCOMPLETE_GLOBAL_VERSION = {(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
                                                             (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff };

    /**
     * Length of the global versions used by <code>FDBRecordVersion</code>.
     */
    public static final int GLOBAL_VERSION_LENGTH = 10;

    /**
     * Length of the underlying byte array representation of an <code>FDBRecordVersion</code>.
     */
    public static final int VERSION_LENGTH = 12;

    /**
     * Minimum possible complete <code>FDBRecordVersion</code> value.
     */
    public static final FDBRecordVersion MIN_VERSION = complete(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                                                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00});

    /**
     * Maximum possible complete <code>FDBRecordVersion</code> value.
     */
    public static final FDBRecordVersion MAX_VERSION = complete(new byte[] {(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
                                                                            (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xfe,
                                                                            (byte)0xff, (byte)0xff});

    @Nonnull private final byte[] versionBytes;
    private final boolean complete;
    private final int localVersion;

    private static boolean isGlobalVersionComplete(@Nonnull byte[] versionBytes) {
        for (int i = 0; i < GLOBAL_VERSION_LENGTH; i++) {
            if (versionBytes[i] != INCOMPLETE_GLOBAL_VERSION[i]) {
                return true;
            }
        }
        return false;
    }

    private static void validateLocalVersion(int localVersion) {
        if (localVersion < 0 || localVersion > 0xffff) {
            throw new RecordCoreException("Specified local version has invalid value " + localVersion);
        }
    }

    private FDBRecordVersion(boolean complete, @Nonnull byte[] versionBytes, boolean copy) {
        if (versionBytes.length != VERSION_LENGTH) {
            throw new RecordCoreException("Specified version has invalid byte length " + versionBytes.length + " != " + VERSION_LENGTH);
        }
        if (complete != isGlobalVersionComplete(versionBytes)) {
            throw new RecordCoreException(complete ? "Specified version has incomplete global version" : "Specified version has a complete global version");
        }
        if (copy) {
            this.versionBytes = Arrays.copyOf(versionBytes, VERSION_LENGTH);
        } else {
            this.versionBytes = versionBytes;
        }
        this.complete = complete;
        this.localVersion = ((versionBytes[GLOBAL_VERSION_LENGTH] & 0xff) << 8) + (versionBytes[GLOBAL_VERSION_LENGTH + 1] & 0xff);
    }

    /**
     * Create a <code>FDBRecordVersion</code> that is complete and has the given
     * global and local versions. It can then be serialized into bytes by
     * combining these bytes in the appropriate way.
     *
     * @param globalVersion the global version associated with this version
     * @param localVersion the local version associated with this version
     * @return a new complete <code>FDBRecordVersion</code>
     */
    @Nonnull
    public static FDBRecordVersion complete(@Nonnull byte[] globalVersion, int localVersion) {
        if (globalVersion.length != GLOBAL_VERSION_LENGTH) {
            throw new RecordCoreException("Specified global version has invalid length " + globalVersion.length);
        }
        if (Arrays.equals(INCOMPLETE_GLOBAL_VERSION, globalVersion)) {
            throw new RecordCoreException("Specified version has incomplete global version");
        }
        validateLocalVersion(localVersion);
        ByteBuffer buffer = ByteBuffer.allocate(VERSION_LENGTH).order(ByteOrder.BIG_ENDIAN);
        buffer.put(globalVersion);
        buffer.putShort((short)localVersion);
        return new FDBRecordVersion(true, buffer.array(), false);
    }

    /**
     * Create a <code>FDBRecordVersion</code> that is complete and has the given
     * serialized bytes. The given bytes should contain both the global
     * version and the local version. Also, bytes will be copied (by default)
     * from the given array when the final object is created. To disable
     * this behavior, consider using the the other version of
     * the {@link #complete(byte[], boolean) complete} static initializer
     * that takes a <code>byte</code> array and a boolean.
     *
     * @param versionBytes the byte representation of a version
     * @return a version object wrapping the given bytes
     */
    @Nonnull
    public static FDBRecordVersion complete(@Nonnull byte[] versionBytes) {
        return complete(versionBytes, true);
    }

    /**
     * Create a <code>FDBRecordVersion</code> that is complete and has
     * the given serialized bytes. The given bytes should contain both
     * the global and the local version. If the <code>copy</code>
     * parameter is set to <code>false</code>, then the new object
     * will use the exact same array as is passed in without copying
     * it. This is unsafe if the array is later modified by something
     * else, so it should only be set to <code>false</code> if one
     * knows that that won't happen.
     *
     * @param versionBytes the byte representation of a version
     * @param copy whether to copy the array for the new <code>FDBRecordVersion</code>
     * @return a version object wrapping the given bytes
     */
    @Nonnull
    public static FDBRecordVersion complete(@Nonnull byte[] versionBytes, boolean copy) {
        return new FDBRecordVersion(true, versionBytes, copy);
    }

    /**
     * Create a <code>FDBRecordVersion</code> that is incomplete and has the given
     * local version. After it is serialized, something else will have to make it
     * sure it gets a global version.
     * @param localVersion the local version associated with this version
     * @return a new incomplete <code>FDBRecordVersion</code>
     */
    @Nonnull
    public static FDBRecordVersion incomplete(int localVersion) {
        validateLocalVersion(localVersion);
        ByteBuffer buffer = ByteBuffer.allocate(VERSION_LENGTH).order(ByteOrder.BIG_ENDIAN);
        buffer.put(INCOMPLETE_GLOBAL_VERSION);
        buffer.putShort((short)localVersion);
        return new FDBRecordVersion(false, buffer.array(), false);
    }

    /**
     * Create a <code>FDBRecordVersion</code> from the given serialized bytes.
     * The given bytes should contain both the global and local version.
     * Unlike {@link #complete(byte[]) complete}, this does not assume
     * that the array represents a complete version, and it will create
     * an incomplete version if the global version is set to all <code>0xff</code> bytes.
     * This method will make a copy of the given array when constituting the
     * new object. To disable this behavior, consider using the version
     * of the {@link #fromBytes(byte[], boolean) fromBytes} static initializer
     * that takes a <code>byte</code> array and a boolean as parameters.
     *
     * @param versionBytes the byte representation of a version
     * @return a version object wrapping the given bytes
     */
    @Nonnull
    public static FDBRecordVersion fromBytes(@Nonnull byte[] versionBytes) {
        return fromBytes(versionBytes, true);
    }

    /**
     * Create a <code>FDBRecordVersion</code> from the given serialized bytes.
     * The given bytes should contain both the global and local version.
     * Unlike {@link #complete(byte[]) complete}, this does not assume
     * that the array represents a complete version, and it will create
     * an incomplete version if the global version is set to all <code>0xff</code> bytes.
     * If the <code>copy</code> parameter is set to <code>false</code>, the
     * new object will use the exact same array as is passed in without
     * copying it. This is unsafe if the array is later modified by
     * something else, so it should only be set if one knows that that
     * won't happen.
     *
     * @param versionBytes the byte representation of a version
     * @param copy whether to copy the array for the new <code>FDBRecordVersion</code>
     * @return a version object wrapping the given bytes
     */
    @Nonnull
    public static FDBRecordVersion fromBytes(@Nonnull byte[] versionBytes, boolean copy) {
        if (versionBytes.length != VERSION_LENGTH) {
            throw new RecordCoreException("Specified version bytes have invalid length " + versionBytes.length);
        }
        boolean complete = isGlobalVersionComplete(versionBytes);
        return new FDBRecordVersion(complete, versionBytes, copy);
    }

    /**
     * Create a <code>FDBRecordVersion</code> from the given {@link Versionstamp}
     * instance. The {@link Versionstamp} class handles version information one
     * layer closer the FoundationDB cluster, and it contains much the same
     * information as this class, so this allows for conversion from that class
     * to this one. By default, this will copy all of the underlying data
     * rather than reusing the same arrays. If one wishes to override this
     * behavior, one should use the version of the
     * {@link #fromVersionstamp(Versionstamp, boolean) fromVersionstamp}
     * static initializer that takes a <code>boolean</code>.
     *
     * @param versionstamp {@link Versionstamp} instance to convert
     * @return a <code>FDBRecordVersion</code> with equivalent information
     */
    @Nonnull
    public static FDBRecordVersion fromVersionstamp(@Nonnull Versionstamp versionstamp) {
        return fromVersionstamp(versionstamp, true);
    }

    /**
     * Create a <code>FDBRecordVersion</code> from the given {@link Versionstamp}
     * instance. The {@link Versionstamp} class handles version information one
     * layer closer the FoundationDB cluster, and it contains much the same
     * information as this class, so this allows for conversion from that class
     * to this one. If the <code>copy</code> parameter is set to <code>false</code>,
     * it will reuse the same underlying array used by the {@link Versionstamp}
     * class rather than making its own. This is unsafe if the array
     * is modified later, so it should only be set to <code>false</code> if
     * one knows that that won't happen.
     *
     * @param versionstamp {@link Versionstamp} instance to convert
     * @param copy whether to copy the underlying array for the new <code>FDBRecordVersion</code>
     * @return a <code>FDBRecordVersion</code> with equivalent information
     */
    @Nonnull
    public static FDBRecordVersion fromVersionstamp(@Nonnull Versionstamp versionstamp, boolean copy) {
        return new FDBRecordVersion(versionstamp.isComplete(), versionstamp.getBytes(), copy);
    }

    /**
     * Return the first <code>FDBRecordVersion</code> that could be written at a given database
     * version or later. The database version referred to here is the eight byte unsigned version
     * that FoundationDB uses to track changes and maintain consistency.
     *
     * @param dbVersion the database version to base this <code>FDBRecordVersion</code> on
     * @return the first version possibly written at this database version or newer
     */
    @Nonnull
    public static FDBRecordVersion firstInDBVersion(long dbVersion) {
        return FDBRecordVersion.complete(
                ByteBuffer.allocate(VERSION_LENGTH).order(ByteOrder.BIG_ENDIAN).putLong(dbVersion).putInt(0).array(),
                false
        );
    }

    /**
     * Return the first <code>FDBRecordVersion</code> that could be written at the given
     * global version.
     *
     * @param globalVersion the global version of the new version
     * @return the first version possibly written at the given global version or newer
     */
    public static FDBRecordVersion firstInGlobalVersion(@Nonnull byte[] globalVersion) {
        return FDBRecordVersion.complete(globalVersion, 0);
    }

    /**
     * Returns the last <code>FDBRecordVersion</code> that could have been written at the given
     * database version. The database version referred to here is the eight byte unsigned version
     * that FoundationDB uses to track changes and maintain consistency.
     *
     * @param dbVersion the database version to base this <code>FDBRecordVersion</code> on
     * @return the last version possibly written at this database version or older
     */
    public static FDBRecordVersion lastInDBVersion(long dbVersion) {
        return FDBRecordVersion.complete(
                ByteBuffer.allocate(VERSION_LENGTH).order(ByteOrder.BIG_ENDIAN).putLong(dbVersion).putInt(-1).array(),
                false
        );
    }

    /**
     * Return the last <code>FDBRecordVersion</code> that could be written at the given
     * global version.
     *
     * @param globalVersion the global version of the new version
     * @return the last version possibly written at the given global version or older
     */
    public static FDBRecordVersion lastInGlobalVersion(@Nonnull byte[] globalVersion) {
        return FDBRecordVersion.complete(globalVersion, 0xffff);
    }

    /**
     * Converts this <code>RecordVersion</code> instance to a
     * {@link Versionstamp} instance. That type is safe to serialize
     * within {@link com.apple.foundationdb.tuple.Tuple}s.
     * By default, it will copy the underlying data arrays
     * when making the new object. If one wishes to disable
     * this behavior, one should consider using the version of
     * {@link #toVersionstamp(boolean) toVersionstamp} that
     * takes a boolean as an argument.
     * @return {@link Versionstamp} representation of this version
     */
    @Nonnull
    public Versionstamp toVersionstamp() {
        return toVersionstamp(true);
    }

    /**
     * Converts this <code>RecordVersion</code> instance to a
     * {@link Versionstamp} instance. That type is safe to serialize
     * within {@link com.apple.foundationdb.tuple.Tuple}s.
     * If the <code>copy</code> parameter is set to <code>false</code>,
     * then the new {@link Versionstamp} will use the same
     * underlying data arrays as this <code>RecordVersion</code>
     * instance. This is only safe if that array is never
     * modified by something else, so it should only be set if
     * one knows that that won't happen.
     *
     * @param copy whether to copy the underlying data arrays for the new instance
     * @return {@link Versionstamp} representation of this version
     */
    @Nonnull
    public Versionstamp toVersionstamp(boolean copy) {
        return Versionstamp.fromBytes(toBytes(copy));
    }

    /**
     * Serializes the version information to a byte array. This
     * array should be used to represent the version within the
     * database. In the case that the version is incomplete, the
     * values within the byte array will not be meaningful except
     * that the last two bytes will represent the local version.
     * By default, this will return a copy of the serialized
     * array from the one used internally by the object, but
     * if one wishes to disable this, consider using the version of
     * {@link #toBytes(boolean) toBytes} that takes a
     * <code>boolean</code> as a parameter.
     *
     * @return byte representation of this version
     */
    @Nonnull
    public byte[] toBytes() {
        return toBytes(true);
    }

    /**
     * Serializes the version information to a byte array. This
     * array should be used to represent the version within the
     * database. In the case that the version is incomplete, the
     * values within the byte array will not be meaningful except
     * that the last two bytes will represent the local version.
     * If the <code>copy</code> parameter is set to <code>false</code>,
     * then the array that is returned will be the exact same array
     * as the one that is used internally by this object, so it
     * is only safe to set <code>copy</code> to <code>false</code>
     * if the array is not modified by anything that uses
     * this call.
     *
     * @param copy whether to copy the underlying data array
     * @return byte representation of this version
     */
    @Nonnull
    @SpotBugsSuppressWarnings(value = "EI", justification = "option is explicitly set to do this")
    public byte[] toBytes(boolean copy) {
        if (copy) {
            return Arrays.copyOf(versionBytes, versionBytes.length);
        } else {
            return versionBytes;
        }
    }

    /**
     * Write the serialized version information to the end of a
     * {@link ByteBuffer}. It will then return the passed in
     * <code>ByteBuffer</code> so that the user can chain updates
     * to it (like many of the mutation methods of a
     * <code>ByteBuffer</code> itself). This method does not itself
     * validate that <code>buffer</code> has sufficient space
     * to store a serialized version but relies on <code>ByteBuffer</code>
     * to throw an error in that case.
     *
     * @param buffer the byte buffer to append this version to
     * @return the same byte buffer
     * @see #toBytes()
     */
    @Nonnull
    public ByteBuffer writeTo(@Nonnull ByteBuffer buffer) {
        return buffer.put(versionBytes);
    }

    /**
     * Write the serialized version to the beginning of the
     * given byte array. This will throw a
     * {@link RecordCoreArgumentException} if there is insufficient
     * space in the array to write this version.
     *
     * @param bytes the byte array in which to write this version
     * @return the first position within <code>bytes</code> after this serialized version
     * @throws RecordCoreArgumentException if there is insufficient space within <code>bytes</code>
     * @see #toBytes()
     * @see #writeTo(byte[], int)
     */
    public int writeTo(@Nonnull byte[] bytes) {
        return writeTo(bytes, 0);
    }

    /**
     * Write the serialized version to a specified offset within
     * the given byte array. This will throw a
     * {@link RecordCoreArgumentException} if there is insufficient
     * space in the array to write this version.
     *
     * @param bytes the byte array in which to write this version
     * @param offset the beginning offset at which to write this version
     * @return the first position within <code>bytes</code> after this serialized version
     * @throws RecordCoreArgumentException if there is insufficient space within <code>bytes</code>
     * @see #toBytes()
     */
    public int writeTo(@Nonnull byte[] bytes, int offset) {
        if (offset < 0 || offset + versionBytes.length > bytes.length) {
            throw new RecordCoreArgumentException("insufficient space in array to write version information");
        }
        System.arraycopy(versionBytes, 0, bytes, offset, versionBytes.length);
        return offset + versionBytes.length;
    }

    /**
     * Whether this version has all of its relevant data set.
     * In particular, this should be set to <code>true</code>
     * if everything about the version is already known.
     * It should be set to <code>false</code> if something
     * is not yet known. It is the part of the version that
     * might be "incomplete" that should be set by some
     * global monotonic order enforcer to impose a global
     * ordering on all record versions.
     * @return whether this record version is complete
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Retrieves the part of the version that is set
     * within a single {@link FDBRecordContext}. This
     * is used to differentiate between records that might
     * be mutated in the same context and requires
     * keeping local state to set the order. Even if
     * a record version is incomplete, it should still
     * return something for this method.
     * @return the local version
     */
    public int getLocalVersion() {
        return localVersion;
    }

    /**
     * Retrieves the part of the version that is set by the database. This
     * will be a byte array of length {@link #GLOBAL_VERSION_LENGTH}. The
     * first eight bytes correspond to the database's version, i.e., the eight
     * byte version used by the database's MVCC system. The last two bytes
     * correspond to an order imposed by the database to create an order
     * between transactions that are committed at the same version.
     * If this {@code FDBRecordVersion} is incomplete, then this will throw
     * an {@link IncompleteRecordVersionException}.
     *
     * @return the part of the version set by the order
     * enforcer
     * @see FDBRecordContext#getCommittedVersion()
     * @see FDBRecordContext#getVersionStamp()
     * @throws IncompleteRecordVersionException if the global version is unset
     */
    @Nonnull
    @SpotBugsSuppressWarnings(value = "EI", justification = "not mutated later")
    public byte[] getGlobalVersion() {
        if (isComplete()) {
            return Arrays.copyOfRange(versionBytes, 0, GLOBAL_VERSION_LENGTH);
        } else {
            throw new IncompleteRecordVersionException();
        }
    }

    /**
     * Retrieves the part of the version that corresponds to the
     * database commit version. This consists of the first eight bytes of the version
     * viewed as a big-endian integer. This is the version used by the database's
     * MVCC system, which means that a transaction should only be able see an
     * {@code FDBRecordVersion} (assuming the version was generated by the transaction's
     * database) if the value returned by this function is less than or equal to the
     * transaction's read version. If this {@code FDBRecordVersion} is incomplete, then
     * this will throw an {@link IncompleteRecordVersionException}.
     *
     * @return the part of the version corresponding to the database version
     * @see FDBRecordContext#getCommittedVersion()
     * @see FDBRecordContext#getReadVersionAsync()
     * @throws IncompleteRecordVersionException if the global version is unset
     */
    public long getDBVersion() {
        if (isComplete()) {
            return ByteBuffer.wrap(versionBytes).order(ByteOrder.BIG_ENDIAN).getLong();
        } else {
            throw new IncompleteRecordVersionException();
        }
    }

    /**
     * Returns an incremented version of the current <code>FDBRecordVersion</code>.
     * In particular, this returns the first possible version that is strictly greater
     * than this instance. This can throw an {@link IllegalStateException} if one
     * tries to increment the maximum version. This function also has no side-effects.
     *
     * @return the next highest <code>FDBRecordVersion</code> instance
     */
    @Nonnull
    public FDBRecordVersion next() {
        if (isComplete()) {
            // This is essentially implementing +1 on a 12 byte unsigned big-endian integer.
            byte[] newVersionBytes = Arrays.copyOf(versionBytes, VERSION_LENGTH);
            boolean stopped = false;
            for (int i = newVersionBytes.length - 1; i >= 0; i--) {
                if (((int) newVersionBytes[i] & 0xff) == 0xff) {
                    newVersionBytes[i] = 0x00;
                } else {
                    newVersionBytes[i] = (byte) (((int) newVersionBytes[i] & 0xff) + 1);
                    stopped = true;
                    break;
                }
            }
            if (!stopped || !isGlobalVersionComplete(newVersionBytes)) {
                throw new RecordCoreException("Attempted to increment maximum version");
            }
            return FDBRecordVersion.complete(newVersionBytes, false);
        } else {
            // Bounds checking is included in the called method.
            return FDBRecordVersion.incomplete(getLocalVersion() + 1);
        }
    }

    /**
     * Returns a decremented version of the current <code>FDBRecordVersion</code>.
     * In particular, this returns the first possible version that is strictly less
     * than this instance. This can throw an {@link IllegalStateException} if one
     * tries to decrement the minimum version. This function also has no side-effects.
     *
     * @return the previous <code>FDBRecordVersion</code> instance
     */
    @Nonnull
    public FDBRecordVersion prev() {
        if (isComplete()) {
            // This is essentially implementing -1 on a 12 byte unsigned big-endian integer.
            byte[] newVersionBytes = Arrays.copyOf(versionBytes, VERSION_LENGTH);
            boolean stopped = false;
            for (int i = newVersionBytes.length - 1; i >= 0; i--) {
                if (newVersionBytes[i] == 0) {
                    newVersionBytes[i] = (byte)0xff;
                } else {
                    newVersionBytes[i] = (byte) (((int) newVersionBytes[i] & 0xff) - 1);
                    stopped = true;
                    break;
                }
            }
            if (!stopped) {
                throw new RecordCoreException("Attempted to decrement minimum version");
            }
            return FDBRecordVersion.complete(newVersionBytes, false);
        } else {
            // Bounds checking is included in the called method.
            return FDBRecordVersion.incomplete(getLocalVersion() - 1);
        }
    }

    /**
     * Complete this version with the version from as successful commit.
     * @param committedVersion the result of {@link FDBRecordContext#getVersionStamp}
     * @return a new record version with a complete version
     */
    @Nonnull
    public FDBRecordVersion withCommittedVersion(@Nullable byte[] committedVersion) {
        if (isComplete()) {
            throw new RecordCoreException("version is already complete");
        }
        if (committedVersion == null) {
            throw new RecordCoreArgumentException("the given committed version was for a read-only transaction");
        }
        return complete(committedVersion, getLocalVersion());
    }

    /**
     * Whether this <code>FDBRecordVersion</code> mathces the given
     * object. It will compare equally with another {@link FDBRecordVersion}
     * that has the same local and global version and will compare
     * as not equal otherwise.
     * @param o the object to check for equality
     * @return <code>true</code> if the object is equal to this version and <code>false</code> otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o == this) {
            return true;
        } else if (!(o instanceof FDBRecordVersion)) {
            return false;
        } else {
            FDBRecordVersion other = (FDBRecordVersion)o;
            if (isComplete() && other.isComplete()) {
                return Arrays.equals(versionBytes, other.toBytes(false));
            } else {
                return !isComplete() && !other.isComplete() && getLocalVersion() == other.getLocalVersion();
            }
        }
    }

    /**
     * Compares two different record versions. The basic contract is
     * that when a series of <code>RecordVersion</code>s are placed
     * in sorted order, the older records should sort before newer
     * records. In particular, the total ordering is as follows:
     *
     *  <ul>
     *      <li>
     *          All complete versions sort before incomplete versions.
     *      </li>
     *      <li>
     *          For two complete versions, the one with the lexicographically
     *          smaller global version sorts earlier. If they have the same
     *          global version, then the one with the smaller local version
     *          sorts earlier.
     *      </li>
     *      <li>
     *          For two incomplete versions, the one with the smaller
     *          local version sorts earlier.
     *      </li>
     *  </ul>
     *
     * @param other the other <code>RecordVersion</code> object to compare to
     * @return -1 if this instance is older, 0 if they are the same, or 1 if this instance is newer
     */
    @Override
    public int compareTo(@Nonnull FDBRecordVersion other) {
        if (isComplete()) {
            if (other.isComplete()) {
                return ByteArrayUtil.compareUnsigned(versionBytes, other.versionBytes);
            } else {
                return -1;
            }
        } else {
            if (other.isComplete()) {
                return 1;
            } else {
                return Integer.compare(getLocalVersion(), other.getLocalVersion());
            }
        }
    }

    /**
     * Hash code derived from the local and global version hash codes. It
     * is the bitwise exclusive-or of the the hash codes of the local and
     * global version components.
     * @return hash code for this version
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(versionBytes);
    }

    /**
     * Human-readable representation of this <code>FDBRecordVersion</code>. It
     * will include both the local and global version information. It will
     * return a global version of 10 consecutive bytes with value <code>0xff</code>.
     * @return human-readable representation of the record version
     */
    @Override
    @Nonnull
    public String toString() {
        return "FDBRecordVersion(" + ByteArrayUtil.printable(versionBytes) + ")";
    }

    /**
     * Exception that can be thrown if one tries to get the global
     * version associated with a version whose global version has
     * not yet been set.
     */
    @SuppressWarnings("serial")
    public static class IncompleteRecordVersionException extends RecordCoreException {
        public IncompleteRecordVersionException() {
            super("Attempted to get global version on incomplete RecordVersion");
        }
    }
}

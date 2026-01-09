/*
 * FormatVersion.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProto;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This version is recorded for each store, and controls certain aspects of the on-disk behavior.
 * <p>
 *     The primary reason for this version is to ensure that if two different versions of code interact with the same
 *     store, the old version won't misinterpret other data in the store. Other than {@link #FORMAT_CONTROL} all of
 *     these can result in a server misunderstanding, and consequently incorrectly updating data in the store.
 * </p>
 * <p>
 *     When the store is opened, the format version on disk will be upgraded to the one provided by
 *     {@link FDBRecordStore.Builder#setFormatVersion}, or {@link #getDefaultFormatVersion()}. There is not currently
 *     a defined policy for increasing the default version, so it is best to set the format version explicitly; see
 *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/709">issue #709</a> for more information.
 * </p>
 * <p>
 *     Generally, if all running instances support a given format version, it is ok to start using it, however upgrading
 *     some format versions may be expensive, especially older versions on larger stores.
 * </p>
 */
@API(API.Status.UNSTABLE)
public enum FormatVersion implements Comparable<FormatVersion> {
    /**
     * Initial FormatVersion.
     */
    INFO_ADDED(1),
    /**
     * This FormatVersion introduces support for tracking record counts as defined by:
     * {@link com.apple.foundationdb.record.RecordMetaData#getRecordCountKey()}.
     */
    RECORD_COUNT_ADDED(2),
    /**
     * This FormatVersion causes the key as defined in {@link com.apple.foundationdb.record.RecordMetaData#getRecordCountKey()} to be stored in the
     * StoreHeader, ensuring that if the key is changed, the record count will be updated.
     * <p>
     *     Unlike indexes, the RecordCountKey does not have a {@code lastModifiedVersion}, and thus the store detects
     *     that the counts need to be rebuilt by checking the key in the StoreHeader.
     * </p>
     * <p>
     *     Warning: There is no way to rebuild the record count key across transactions, and it does not check the
     *     {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.UserVersionChecker}, so changing the record count key will cause the store to attempt to rebuild the
     *     counts when opening the store. If you have not started using this version (or
     *     {@link #RECORD_COUNT_ADDED}), you may want to consider replacing the RecordCountKey with a
     *     {@link com.apple.foundationdb.record.metadata.IndexTypes#COUNT} index first.
     * </p>
     */
    RECORD_COUNT_KEY_ADDED(3),
    /**
     * This FormatVersion was introduced to support testing of upgrading the format version past
     * {@link #RECORD_COUNT_KEY_ADDED}, but does not change the behavior of the store.
     */
    FORMAT_CONTROL(4),
    /**
     * This FormatVersion causes all stores to store the split suffix, even if
     * {@link com.apple.foundationdb.record.RecordMetaData#isSplitLongRecords()} is {@code false}, unless
     * {@link com.apple.foundationdb.record.RecordMetaDataProto.DataStoreInfo#getOmitUnsplitRecordSuffix()} is {@code true} on the StoreHeader.
     * <p>
     *     In order to maintain backwards compatibility, and not require rewriting all the records, if upgrading from an
     *     earlier FormatVersion to this one, if the metadata does not allow splitting long records,
     *     {@linkplain com.apple.foundationdb.record.RecordMetaDataProto.DataStoreInfo#getOmitUnsplitRecordSuffix() getOmitUnsplitRecordSuffix()}
     *     will be set to {@code true} on the StoreHeader.
     * </p>
     * <p>
     *     By always including the suffix, it allows a couple benefits:
     *     <ul>
     *         <li>The metadata will be able to change to support splitting long records in the future</li>
     *         <li>When upgrading to {@link #SAVE_UNSPLIT_WITH_SUFFIX} it can store the versions adjacent
     *         to the record, rather than a separate sub-range.</li>
     *     </ul>
     * </p>
     */
    SAVE_UNSPLIT_WITH_SUFFIX(5),
    /**
     * This FormatVersion causes the record versions (if enabled via {@link com.apple.foundationdb.record.RecordMetaData#isStoreRecordVersions()})
     * to be stored adjacent to the record itself, rather than in a separate sub-range.
     * <p>
     *     This most notably improves the performance or load of reading records, particularly a range of records, as it
     *     doesn't have to do a separate read to get the versions.
     * </p>
     * <p>
     *     Note: If the store is omitting the unsplit record suffix due to
     *     {@link com.apple.foundationdb.record.RecordMetaDataProto.DataStoreInfo#getOmitUnsplitRecordSuffix()}, this will continue to store the
     *     record versions in the separate space.
     * </p>
     * <p>
     *     Warning: If {@link com.apple.foundationdb.record.RecordMetaData#isStoreRecordVersions()} is enabled when upgrading to this version,
     *     the code will try to move the versions transactionally when opening the store.
     * </p>
     */
    SAVE_VERSION_WITH_RECORD(6),
    /**
     * This FormatVersion allows the record state to be cached and invalidated with the meta-data version key.
     * <p>
     *     With this version, every time the {@link com.apple.foundationdb.record.RecordStoreState}
     *     (storeHeader and index states) is updated for a store marked as cacheable, the meta-data-version for the cluster will be bumped,
     *     and all cache entries for all stores in the cluster are invalidated. Consequently if a store did not know
     *     that a store was cacheable, when it was, it could update the store header or index state, without
     *     invalidating the cache, causing others to interact with the store using an old version of the metadata, or
     *     treating an index as disabled.
     * </p>
     * @see com.apple.foundationdb.record.provider.foundationdb.storestate.MetaDataVersionStampStoreStateCache
     * @see FDBRecordStore#setStateCacheability
     */
    CACHEABLE_STATE(7),
    /**
     * This FormatVersion allows the user to store additional fields in the StoreHeader.
     * These fields aren't used by the record store itself, but allow the user to set and read additional information.
     * @see FDBRecordStore#setHeaderUserField
     *
     */
    HEADER_USER_FIELDS(8),
    /**
     * This FormatVersion allows the store to mark indexes as {@link com.apple.foundationdb.record.IndexState#READABLE_UNIQUE_PENDING} if
     * appropriate.
     */
    READABLE_UNIQUE_PENDING(9),
    /**
     * This FormatVersion allows building non-idempotent indexes (e.g. COUNT) from a source index.
     */
    CHECK_INDEX_BUILD_TYPE_DURING_UPDATE(10),
    /**
     * This FormatVersion allows setting a state for the RecordCountKey on an individual store.
     * @see RecordMetaDataProto.DataStoreInfo#getRecordCountState()
     */
    RECORD_COUNT_STATE(11),
    /**
     * This FormatVersion supports setting a store lock state.
     */
    STORE_LOCK_STATE(12),
    /**
     * This FormatVersion supports setting an incarnation for the store.
     * The incarnation is intended to be incremented when moving data from one cluster to another.
     * @see RecordMetaDataProto.DataStoreInfo#getIncarnation()
     */
    INCARNATION(13),
    ;

    private final int value;

    private static final Map<Integer, FormatVersion> VERSIONS = Arrays.stream(values())
            .collect(Collectors.toUnmodifiableMap(FormatVersion::getValueForSerialization,
                    version -> version));

    private static final FormatVersion MAX_SUPPORTED_VERSION = Arrays.stream(values())
            .max(Comparator.naturalOrder())
            .orElseThrow(); // will throw if there are on enum values


    FormatVersion(final int value) {
        this.value = value;
    }

    /**
     * The minimum {@code FormatVersion}.
     * @return the minimum {@code FormatVersion}
     */
    static FormatVersion getMinimumVersion() {
        return INFO_ADDED;
    }

    /**
     * The maximum {@code FormatVersion} that this version of the Record Layer can support.
     * @return the maximum supported version
     */
    public static FormatVersion getMaximumSupportedVersion() {
        return MAX_SUPPORTED_VERSION;
    }

    /**
     * The default FormatVersion that this code will set when opening a record store, if the user does not call
     * {@link FDBRecordStore.Builder#setFormatVersion}.
     * <p>
     *     Note: We don't currently have a well-defined policy for updating this, see
     *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/709">Issue #709</a>.
     * </p>
     */
    public static FormatVersion getDefaultFormatVersion() {
        return CACHEABLE_STATE;
    }

    @API(API.Status.INTERNAL)
    int getValueForSerialization() {
        return value;
    }

    @API(API.Status.INTERNAL)
    static void validateFormatVersion(int candidateVersion, final SubspaceProvider subspaceProvider) {
        if (candidateVersion < getMinimumVersion().getValueForSerialization() ||
                candidateVersion > getMaximumSupportedVersion().getValueForSerialization()) {
            throw new UnsupportedFormatVersionException("Unsupported format version " + candidateVersion,
                    subspaceProvider.logKey(), subspaceProvider);
        }
    }

    @Nonnull
    @API(API.Status.INTERNAL)
    public static FormatVersion getFormatVersion(int candidateVersion) {
        final FormatVersion candidate = VERSIONS.get(candidateVersion);
        if (candidate == null) {
            throw new UnsupportedFormatVersionException("Unsupported format version " + candidateVersion);
        }
        return candidate;
    }

    /**
     * Returns whether this version is {@code >=} the provided version.
     * @param other a different format version.
     * @return {@code true} if this format version is the same as {@code other} or greater than it
     */
    public boolean isAtLeast(final FormatVersion other) {
        return this.compareTo(other) >= 0;
    }
}

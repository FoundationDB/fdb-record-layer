/*
 * SplitHelperTestConfig.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.FDBRecordStoreProperties;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Versionstamp;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.Stream;

public class SplitHelperTestConfig {
    public final boolean splitLongRecords;
    public final boolean omitUnsplitSuffix;
    public final boolean unrollRecordDeletes;
    public final boolean loadViaGets;
    public final boolean isDryRun;
    public final boolean useVersionInKey;

    public SplitHelperTestConfig(boolean splitLongRecords, boolean omitUnsplitSuffix, boolean unrollRecordDeletes,
                                 boolean loadViaGets, boolean isDryRun, boolean useVersionInKey) {
        this.splitLongRecords = splitLongRecords;
        this.omitUnsplitSuffix = omitUnsplitSuffix;
        this.unrollRecordDeletes = unrollRecordDeletes;
        this.loadViaGets = loadViaGets;
        this.isDryRun = isDryRun;
        this.useVersionInKey = useVersionInKey;
    }

    public SplitKeyValueHelper keyHelper(int localVersion) {
        if (useVersionInKey) {
            return new VersioningSplitKeyValueHelper(Versionstamp.incomplete(localVersion));
        } else {
            return DefaultSplitKeyValueHelper.INSTANCE;
        }
    }

    @Nonnull
    public RecordLayerPropertyStorage.Builder setProps(@Nonnull RecordLayerPropertyStorage.Builder props) {
        return props
                .addProp(FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES, unrollRecordDeletes)
                .addProp(FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS, loadViaGets);
    }

    public boolean hasSplitPoints() {
        return splitLongRecords || !omitUnsplitSuffix;
    }

    @Override
    public String toString() {
        return "SplitHelperTestConfig{" +
                "splitLongRecords=" + splitLongRecords +
                ", omitUnsplitSuffix=" + omitUnsplitSuffix +
                ", unrollRecordDeletes=" + unrollRecordDeletes +
                ", loadViaGets=" + loadViaGets +
                ", isDryRun=" + isDryRun +
                ", useVersionInKey=" + useVersionInKey +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SplitHelperTestConfig that = (SplitHelperTestConfig)o;
        return splitLongRecords == that.splitLongRecords && omitUnsplitSuffix == that.omitUnsplitSuffix &&
                unrollRecordDeletes == that.unrollRecordDeletes && loadViaGets == that.loadViaGets &&
                isDryRun == that.isDryRun && useVersionInKey == that.useVersionInKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitLongRecords, omitUnsplitSuffix, unrollRecordDeletes, loadViaGets, useVersionInKey);
    }

    public static Stream<SplitHelperTestConfig> allValidConfigs() {
        // Note that splitLongRecords="true" && omitUnsplitSuffix="true" is not valid
        return Stream.of(false, true).flatMap(useVersionInKey ->
                Stream.of(false, true).flatMap(splitLongRecords ->
                        (splitLongRecords ? Stream.of(false) : Stream.of(false, true)).flatMap(omitUnsplitSuffix ->
                                Stream.of(false, true).flatMap(unrollRecordDeletes ->
                                        Stream.of(false, true).flatMap(loadViaGets ->
                                                Stream.of(false, true).map(isDryRun ->
                                                        new SplitHelperTestConfig(splitLongRecords, omitUnsplitSuffix, unrollRecordDeletes, loadViaGets, isDryRun, useVersionInKey)))))));
    }

    public static Stream<SplitHelperTestConfig> getConfigsNoDryRun() {
        return allValidConfigs().filter(config -> !config.isDryRun);
    }

    public static Stream<SplitHelperTestConfig> getConfigsNoVersionInKey() {
        return allValidConfigs().filter(config -> !config.useVersionInKey);
    }

    public static SplitHelperTestConfig getDefault() {
        return new SplitHelperTestConfig(true, false,
                FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES.getDefaultValue(),
                FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS.getDefaultValue(), false, false);
    }
}

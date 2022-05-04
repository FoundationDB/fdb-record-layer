/*
 * APIVersion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

/**
 * An enum representing the different supported FDB API versions. The
 * <a href="https://apple.github.io/foundationdb/api-general.html#api-versions">FDB API version</a>
 * controls the minimum FDB server version that can be successfully connected to, as well as
 * what features are supported by the FDB client. It is therefore important that in scenarios where
 * there are FDB servers on different versions that the API version be configured to not exceed the
 * smallest FDB server version. If the API version is set too high, the FDB client may hang when
 * trying to connect to the server.
 *
 * <p>
 * Because FDB API versions also control which features are guaranteed to be supported, there may
 * be Record Layer features that are gated on specific FDB API versions. For that reason, if there
 * is a new feature that relies on specific FDB server features being available, the client may
 * need to be configured with the appropriate API version before that feature can be used.
 * </p>
 *
 * <p>
 * A note on Record Layer API stability: the elements of this enum are expected to shift as support
 * for FDB versions are deprecated and removed. As a result, the elements of this enum will be added
 * and removed as support for old FDB versions is dropped and new FDB versions is added.
 * </p>
 *
 * @see FDBDatabaseFactory#setAPIVersion(APIVersion)
 */
@API(API.Status.MAINTAINED)
public enum APIVersion {
    /**
     * API version corresponding to FoundationDB 6.3.
     */
    API_VERSION_6_3(630),
    /**
     * API version corresponding to FoundationDB 7.0.
     */
    API_VERSION_7_0(700),
    /**
     * API version corresponding to FoundationDB 7.1.
     */
    API_VERSION_7_1(710),
    ;

    private final int versionNumber;

    APIVersion(int versionNumber) {
        this.versionNumber = versionNumber;
    }

    /**
     * Get the API version number. This is the value passed to FDB during client initialization.
     * @return the version number associated with this version of the API
     */
    public int getVersionNumber() {
        return versionNumber;
    }

    /**
     * Get the default API version. Unless the adopter overrides this value by calling
     * {@link FDBDatabaseFactory#setAPIVersion(APIVersion)}, this is the API version that will
     * be used to initialize the FDB client connection.
     *
     * <p>
     * This is currently {@link #API_VERSION_6_3}.
     * </p>
     *
     * @return the default API version
     */
    public static APIVersion getDefault() {
        return API_VERSION_6_3;
    }

    /**
     * Get the {@code APIVersion} enum from the version number.
     * @param versionNumber the FDB API version number
     * @return the corresponding enum value
     */
    public static APIVersion fromVersionNumber(int versionNumber) {
        for (APIVersion version : values()) {
            if (version.getVersionNumber() == versionNumber) {
                return version;
            }
        }
        throw new RecordCoreArgumentException("api version not supported");
    }
}

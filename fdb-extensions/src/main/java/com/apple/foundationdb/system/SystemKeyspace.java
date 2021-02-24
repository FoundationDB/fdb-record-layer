/*
 * SystemKeyspace.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.system;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;

/**
 * Keys living in the FDB system and special keyspace. These keys are used to communicate system configuration
 * information and related information between the FoundationDB server and client. This API is {@link API.Status#INTERNAL}
 * and should not be consumed directly by adopters. They should instead generally be referenced through wrapper
 * functions that exist in this project and in the Record Layer core project.
 *
 * <p>
 * There is currently a project to provide a framework for FoundationDB clients to make use of the special
 * key space API. As that project develops, this class may become unnecessary. See:
 * <a href="https://github.com/apple/foundationdb/blob/master/design/special-key-space.md">Special-Key-Space</a>.
 * </p>
 */
@SpotBugsSuppressWarnings(value = "MS_MUTABLE_ARRAY", justification = "array copying is expensive")
@API(API.Status.INTERNAL)
public class SystemKeyspace {

    // Key prefixes for the various system and special key subspaces

    /**
     * The prefix under which all FDB system and special data live. Regular user operations typically cannot read keys
     * with this prefix unless they set special options except in special circumstances.
     */
    private static final byte[] SYSTEM_PREFIX = {(byte)0xff};

    /**
     * The prefix under which FDB system data that is sharded like regular data lives. The data in the FDB system
     * keyspace that begins with {@code \xff} but not {@code \xff\x02} is handled somewhat specially as certain database
     * operations rely on knowing the contents during recoveries, etc. However, because those data are not sharded, they
     * are fundamentally limited in size. So, to allow for more system data to be stored in FDB, the {@code \xff\x02}
     * keyspace exists, which are handled more like regular data (in that placement and shard splitting or merging are
     * handled by the data distributor). For a client, this is mostly just trivia, but it seems necessary to explain why
     * certain keys are prefixed like this.
     */
    private static final byte[] SYSTEM_2_PREFIX = {(byte)0xff, 0x02};

    /**
     * The prefix under which "special" keys live. The data in the range {@code \xff\xff} does not "really" exist,
     * but instead acts as instructions to the client used to retrieve some special information.
     * For example, they might be used to read and return client configuration information. They
     * may not read any actual data from the database or (sometimes) even make any network calls at all.
     * Note: This is in contrast to the rest of {@code \xff}, which is stored in the database, but is off limits to
     * regular client operations. 
     */
    private static final byte[] SPECIAL_PREFIX = {(byte)0xff, (byte)0xff};

    // System keys

    public static final byte[] METADATA_VERSION_KEY = systemPrefixedKey("/metadataVersion");
    public static final byte[] PRIMARY_DATACENTER_KEY = systemPrefixedKey("/primaryDatacenter");

    public static final byte[] TIMEKEEPER_KEY_PREFIX = system2PrefixedKey("/timeKeeper/map/");
    public static final byte[] CLIENT_LOG_KEY_PREFIX = system2PrefixedKey("/fdbClientInfo/client_latency/");

    // Special keys

    public static final byte[] CONNECTION_STR_KEY = specialPrefixedKey("/connection_string");
    public static final byte[] CLUSTER_FILE_PATH_KEY = specialPrefixedKey("/cluster_file_path");

    private static byte[] systemPrefixedKey(@Nonnull String key) {
        return ByteArrayUtil.join(SYSTEM_PREFIX, key.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] system2PrefixedKey(@Nonnull String key) {
        return ByteArrayUtil.join(SYSTEM_2_PREFIX, key.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] specialPrefixedKey(@Nonnull String key) {
        return ByteArrayUtil.join(SPECIAL_PREFIX, key.getBytes(StandardCharsets.US_ASCII));
    }

    private SystemKeyspace() {
    }
}

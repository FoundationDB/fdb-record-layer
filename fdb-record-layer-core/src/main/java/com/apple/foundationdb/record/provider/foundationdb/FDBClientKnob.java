/*
 * FDBClientKnob.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A curated list of FDB client knobs that adopters may want to tune, along with the type of value that each
 * knob expects. The FDB native client is configured through a large number of "knobs," most of which are only
 * of interest to FDB developers, but some of which can be useful for client tuning. Under the cover, knobs
 * are set via the {@link com.apple.foundationdb.NetworkOptions#setKnob(String)} API, which takes a single
 * string-valued argument that should be of the form {@code knob_name=knob_value}. This enum exists to make
 * setting the more common knobs less error-prone by allowing the {@link FDBDatabaseFactory#setKnob(FDBClientKnob, String)}
 * API to validate that the supplied value is of the type that the knob actually expects. It will then construct
 * the appropriate argument value and pass it to the underlying {@code NetworkOptions}.
 *
 * <p>
 * Not every knob supported by the native client has a constant here. For knobs that are not (yet) enumerated,
 * {@link FDBDatabaseFactory#setKnob(String, String)} can be used instead. Note that no validation is performed
 * on the value being set for any knob not in this enumeration. However, the fact that this enumeration is
 * not exhaustive means that the client does not need to wait for us to release a new Record Layer version every
 * time a new client knob is added to FDB. Any knob that is supported by the linked client library can be configured
 * by the library consumer.
 * </p>
 *
 * <p>
 * This class is {@link API.Status#UNSTABLE}. It may add new values at any time in response to new knob values
 * that are determined to be useful to configure. The name of each enum constant is the upper-case
 * version of the associated knob's name. This mapping is relied on to derive the knob name, so this
 * convention must be preserved for any new constants that are added.
 * </p>
 *
 * <p>
 * For the full list of available knobs, consult the FDB source files:
 * </p>
 *
 * <ul>
 *     <li><a href="https://github.com/apple/foundationdb/blob/main/flow/include/flow/Knobs.h"><code>flow/Knobs.h</code></a> - Common knobs available in all FDB processes</li>
 *     <li><a href="https://github.com/apple/foundationdb/blob/main/flow/Knobs.cpp"><code>flow/Knobs.cpp</code></a> - For common knobs' default values</li>
 *     <li><a href="https://github.com/apple/foundationdb/blob/main/fdbclient/include/fdbclient/Knobs.h"><code>fdbclient/Knobs.h</code></a> - Knobs specific to FDB clients</li>
 *     <li><a href="https://github.com/apple/foundationdb/blob/main/fdbclient/ClientKnobs.cpp"><code>fdbclient/ClientKnobs.cpp</code></a> - For the client knobs' default values</li>
 * </ul>
 *
 * <p>
 * When consulting those files, be sure to validate that the knob is present in the tag corresponding to the
 * FDB client version configured to run with this client. The knobs in this class are annotated with
 * their required FDB client versions. Any knob without an annotation has been around since at least 7.0.0.
 * </p>
 *
 * @see FDBDatabaseFactory#setKnob(FDBClientKnob, String)
 * @see FDBDatabaseFactory#setKnob(String, String)
 * @see com.apple.foundationdb.NetworkOptions#setKnob(String)
 */
@API(API.Status.UNSTABLE)
public enum FDBClientKnob {
    /**
     * Maximum size in bytes of a single network packet. If a single packet exceeds this size, the
     * network request will throw a {@link com.apple.foundationdb.FDBError#PLATFORM_ERROR}.
     *
     * @see #PACKET_WARNING
     */
    PACKET_LIMIT(KnobValueType.LONG),
    /**
     * Warning threshold in bytes for network packets. If a single packet exceeds this size, it
     * will log a {@code LargePacketSent} trace event.
     *
     * @see #PACKET_LIMIT
     */
    PACKET_WARNING(KnobValueType.LONG),
    /**
     * The number of dedicated threads used by the client to perform TLS handshakes. If set to {@code 0}, TLS
     * handshakes are instead performed on the main network thread.
     */
    TLS_CLIENT_HANDSHAKE_THREADS(KnobValueType.INT),
    /**
     * If {@code true}, TLS handshakes are never performed on the main network thread, even if
     * {@link #TLS_CLIENT_HANDSHAKE_THREADS} is set to {@code 0}.
     *
     * @since FDB 8.0.0
     */
    DISABLE_MAINTHREAD_TLS_HANDSHAKE(KnobValueType.BOOLEAN),
    /**
     * The number of times a TLS connection to a given peer is allowed to fail within
     * {@link #TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT} seconds before further connection attempts to that peer
     * are throttled.
     */
    TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS(KnobValueType.INT),
    /**
     * The time window, in seconds, over which failed TLS connection attempts to a given peer are counted for
     * the purposes of {@link #TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS}.
     */
    TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT(KnobValueType.DOUBLE),
    /**
     * The maximum number of commit proxies that the client will maintain connections to at once.
     */
    MAX_COMMIT_PROXY_CONNECTIONS(KnobValueType.INT),
    /**
     * The maximum number of GRV (get read version) proxies that the client will maintain connections to at once.
     */
    MAX_GRV_PROXY_CONNECTIONS(KnobValueType.INT),
    /**
     * The number of seconds to wait after failing to reach an endpoint before the client's location cache will
     * retry that endpoint again.
     *
     * @since FDB 7.1.0
     */
    LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL(KnobValueType.DOUBLE),
    /**
     * Whether the client should log detailed information about connection attempts. If enabled, logs are
     * written to the directory specified by {@link #CONNECTION_LOG_DIRECTORY}.
     *
     * @since FDB 7.4.0
     */
    LOG_CONNECTION_ATTEMPTS_ENABLED(KnobValueType.BOOLEAN),
    /**
     * The directory to which connection attempt logs are written, if {@link #LOG_CONNECTION_ATTEMPTS_ENABLED}
     * is set to {@code true}.
     *
     * @since FDB 7.4.0
     */
    CONNECTION_LOG_DIRECTORY(KnobValueType.STRING),
    /**
     * The maximum time, in seconds, that the client will wait between attempts to reconnect to a peer,
     * following exponential backoff.
     */
    MAX_RECONNECTION_TIME(KnobValueType.DOUBLE),
    /**
     * The rate at which the delay between successive reconnection attempts to an unreachable peer grows.
     */
    RECONNECTION_TIME_GROWTH_RATE(KnobValueType.DOUBLE),
    /**
     * The amount of time, in seconds, since the last connection attempt to a peer after which the reconnection
     * delay is reset back to its initial value rather than continuing to grow.
     */
    RECONNECTION_RESET_TIME(KnobValueType.DOUBLE),
    /**
     * Whether the client should proactively evict a storage server address from its location cache once that
     * address's persistent connection failures cross {@link #LOCATION_CACHE_PEER_EVICTOR_FAILED_THRESHOLD}.
     * The location cache maintains a list of storage servers that are believed to serve data for different
     * ranges of data. When this knob is enabled, it allows the client to clean up any connections to
     * instances that it considers to be down as it has been unable to make a healthy connection.
     *
     * @see #LOCATION_CACHE_PEER_EVICTOR_FAILED_THRESHOLD
     * @see #LOCATION_CACHE_PEER_EVICTOR_DELAY
     * @see #LOCATION_CACHE_PEER_EVICTOR_ENABLED
     * @see #LOCATION_CACHE_PEER_EVICTOR_SCAN_CHUNK
     * @since FDB 7.3.78 and 7.4.7
     */
    LOCATION_CACHE_PEER_EVICTOR_ENABLED(KnobValueType.BOOLEAN),
    /**
     * Time in seconds between sweeps of the location cache peer eviction sweep. If location
     * cache peer eviction is not enabled, this has no effect. Otherwise, the client will run a background
     * process to clean up its internal state, with the period determined by this knob.
     *
     * @see #LOCATION_CACHE_PEER_EVICTOR_ENABLED
     * @since FDB 7.3.78 and 7.4.7
     */
    LOCATION_CACHE_PEER_EVICTOR_DELAY(KnobValueType.DOUBLE),
    /**
     * The number of unsuccessful peer connection attempts to require before it is eligible to evicted
     * from the location cache. If set to zero, then any failure makes the entry in the location cache
     * eligible for clean up.
     *
     * @see #LOCATION_CACHE_PEER_EVICTOR_ENABLED
     * @since FDB 7.3.78 and 7.4.7
     */
    LOCATION_CACHE_PEER_EVICTOR_FAILED_THRESHOLD(KnobValueType.INT),
    /**
     * The number of location cache ranges that the address-based invalidation scan performed by the
     * location-cache peer evictor processes before yielding to the main event loop. This has no
     * effect if location cache peer eviction is not enabled. It is present to prevent slow tasks
     * affecting other operations during the peer eviction.
     *
     * @see #LOCATION_CACHE_PEER_EVICTOR_ENABLED
     * @since FDB 7.3.78 and 7.4.7
     */
    LOCATION_CACHE_PEER_EVICTOR_SCAN_CHUNK(KnobValueType.INT),
    /**
     * Whether the client should clear its cached sampled subset of commit/GRV proxies whenever the recruited
     * proxy count drops below {@link #MAX_COMMIT_PROXY_CONNECTIONS} or {@link #MAX_GRV_PROXY_CONNECTIONS}, so
     * that the cache cleans up connections to unresponsive proxies.
     *
     * @since FDB 7.3.78 and 7.4.7
     */
    SHRINK_PROXY_LIST_CLEAR_CACHE_BELOW_THRESHOLD(KnobValueType.BOOLEAN),
    ;

    @Nonnull
    private static final Map<String, FDBClientKnob> BY_KNOB_NAME = Arrays.stream(values())
            .collect(Collectors.toMap(FDBClientKnob::getKnobName, Function.identity()));

    @Nonnull
    private final KnobValueType valueType;
    @Nonnull
    private final String knobName;

    FDBClientKnob(@Nonnull KnobValueType valueType) {
        this.valueType = valueType;
        this.knobName = name().toLowerCase(Locale.ROOT);
    }

    /**
     * Get the type of value that this knob expects.
     *
     * @return the type of value that this knob expects
     */
    @Nonnull
    public KnobValueType getValueType() {
        return valueType;
    }

    /**
     * Get the name of the knob, as it should be supplied to the native client. This is just the name of the
     * enum constant, lower-cased.
     *
     * @return the name of the knob
     */
    @Nonnull
    public String getKnobName() {
        return knobName;
    }

    /**
     * Look up the {@link FDBClientKnob} constant with the given (lower-case) knob name, if one exists.
     *
     * @param knobName the (lower-case) name of the knob to look up
     * @return the {@link FDBClientKnob} constant associated with {@code knobName}, or {@code null} if
     * {@code knobName} is not one of the knobs enumerated by this class
     */
    @Nullable
    public static FDBClientKnob fromKnobName(@Nonnull String knobName) {
        return BY_KNOB_NAME.get(knobName);
    }

    /**
     * The type of value that a given {@link FDBClientKnob} expects to be set to. This is used to validate that
     * a value supplied to one of the {@link FDBDatabaseFactory#setKnob(FDBClientKnob, String)}-style setters
     * matches what the underlying native client knob actually expects. Different types may restrict the set
     * of legal values to only those that can be interpreted correctly by the native client. See each knob value
     * type's documentation for more details about the range of values they accept.
     */
    public enum KnobValueType {
        /**
         * A 32-bit integer knob value type. Accepts any value accepted by {@link Integer#decode(String)}, except
         * for the {@code #}-prefixed hex notation that those methods accept but the native client does not.
         * In particular, this includes ordinary base-10 values (e.g., {@code "123"}, {@code "-1"}), hex values
         * with a {@code 0x} or {@code 0X} prefix (e.g., {@code "0x7B"}), and octal values with a leading {@code 0}
         * (e.g., {@code "0173"}, interpreted as decimal {@code 123}), matching the parsing performed by the native client.
         */
        INT,
        /**
         * A 64-bit integer knob value type. Accepts a similar set of values as {@link #INT}, except using
         * {@link Long#decode(String)}, thus allowing a larger range of integral values.
         */
        LONG,
        /**
         * A double-precision floating point knob value type. Accepts any value accepted by {@link Double#parseDouble(String)}.
         */
        DOUBLE,
        /**
         * A Boolean knob value type. Accepts {@code "true"} or {@code "false"} (case-insensitively),
         * or any value that can be parsed as an integer (as with {@link #INT}), with a non-zero value interpreted
         * as {@code true} and zero interpreted as {@code false}.</td></tr>
         */
        BOOLEAN,
        /**
         * A {@link String} knob value type. Accepts any string.
         */
        STRING,
    }
}

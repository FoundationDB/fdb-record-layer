/*
 * package-info.java
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

/**
 * Maintain leaderboard as multiple time-windowed ranked sets (so that old scores fall off).
 *
 * <p>
 * The time windows are grouped by type, a client-supplied positive integer. For example, daily and weekly.
 * The built-in <code>ALL_TIME_WINDOW_LEADERBOARD_TYPE</code> type has the value 0.
 * </p>
 *
 * <p>
 * Each time window has a start and (exclusive) end timestamp, using a client-supplied timebase.
 * </p>
 *
 * <p>
 * A record is expected to contain multiple timestamped scored. The leaderboard enters the record's best score
 * (client specifies whether this is lowest or highest numerical value) in each time window. This determines
 * the record's rank (leaderboard position) at that time.
 * </p>
 *
 * <h3>Index Representation</h3>
 *
 * The expected index keys are <code>group_fields..., score, timestamp, more_fields...</code>.
 * A single B-tree in the <em>primary</em> index subspace stores these once for all time windows.
 *
 * <p>
 * The ideal root expression is therefore <code>field(scores_field, fanOut).nest(concat(score, timestamp, context))</code>.
 * </p>
 *
 * <p>
 * For the sake of simpler type systems, {@link com.apple.foundationdb.record.metadata.expressions.SplitKeyExpression} can be used to store
 * timestamped scores in the record in a repeated long field, along with zero or more opaque context values.
 * </p>
 *
 * <p>
 * The index root expression is then <code>split(list_field/fanOut, n)</code> or <code>concat(group_fields..., split(list_field/fanOut, n), more_fields...)</code>.
 * </p>
 *
 * <p>
 * The primary index entries are <code>group_fields..., score, timestamp, more_fields..., primary_key</code> &rarr; <code>context_values</code>.
 * </p>
 *
 * <p>
 * The root of the <em>secondary</em> index subspace is a directory, {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardDirectory}, serialized as Protobuf.
 * </p>
 *
 * <p>
 * For each directory entry, there is a leaderboard subspace using the entry's {@linkplain com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboard#getSubspaceKey() subspace key}.
 * </p>
 *
 * <p>
 * There is a {@link com.apple.foundationdb.async.RankedSet} within the leaderboard subspace for each group, that is, with any grouping keys as a prefix.
 * </p>
 *
 * <p>
 * The ranked set entries are for <code>score, timestamp, more_fields</code>.
 * When scores match, the timestamp can break the tie: earlier is better (even when high scores come first).
 * <code>more_fields</code> can be used to further break ties at the same timestamp.
 * </p>
 *
 * <p>
 * A group can have an optional {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardSubDirectory}.
 * A sub-directory allows individual groups to have different settings for whether high scores come first.
 * Sub-directories are serialized as Protobuf in the <em>secondary</em> index subspace with key <code>[null, group_fields...]</code>.
 * This does not conflict with leaderboards because <code>null</code> is not a valid leaderboard subspace key (they are automatically assigned integers).
 * </p>
 *
 * <h3>Operations</h3>
 *
 * <b>Updating Time Windows</b>
 *
 * The client is also responsible for keeping a current set of time windows active.
 * This is done with {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardWindowUpdate}.
 * The caller specifies:<ul>
 * <li>Whether high scores are better or low scores for determining the best score in a given window</li>
 * <li>A timestamp before which expired time windows can be removed from the database</li>
 * <li>A set of per-type specifications for regularly spaced windows, giving a base timestamp, duration and repeat count</li>
 * </ul>
 *
 * <p>
 * A good practice is to probabilistically add time windows in the future, with the chances increasing to certainty
 * as the time when a new window would be needed approaches.
 * </p>
 *
 * <h3>Scanning</h3>
 *
 * The leaderboard index can be scanned <code>BY_VALUE</code>, like an ordinary index, provided the all-time time window
 * is maintained. This means by score ranges, or score matches for time ranges.
 *
 * <p>
 * Scanning <code>BY_RANK</code> means a range of ranks rather than scores, as with a rankset index.
 * For this, too, the <code>ALL_TIME_LEADERBOARD_TYPE</code> time window is used.
 * </p>
 *
 * <p>
 * Scanning <code>BY_TIME_WINDOW</code> adds to tuple items at the beginning of the range representing the time window type and target timestamp.
 * The oldest ranked set of the given type containing the timestamp will be used.
 * For example,
 * </p>
 * <code>
 *     final TupleRange top_10_type_2 = new TupleRange(Tuple.from(2, now, 0), Tuple.from(2, now, 9), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
 *     final RecordCursor&lt;Message&gt; cursor = &lt;Message&gt;recordStore.scanIndexRecords("LeaderboardIndex", IndexScanType.BY_TIME_WINDOW, top_2_type_2, ScanProperties.FORWARD_SCAN);
 * </code>
 *
 * <h3>Querying</h3>
 *
 * The index can be used to match predicates / sorting for <code>Query.rank(expr)</code>.
 * Again, the <code>ALL_TIME_LEADERBOARD_TYPE</code> time window is used.
 *
 * <p>
 * It can also match <code>Query.timeWindowRank(leaderboardType, leaderboardTimestamp, expr)</code>.
 * <code>leaderboardType</code> and <code>leaderboardTimestamp</code> can be strings, in which case
 * they specify the names of runtime parameters containing those values. The rank(s) will be taken from
 * the oldest leaderboard of the specified type containing the specified timestamp.
 * </p>
 *
 */

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;


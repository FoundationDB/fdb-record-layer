/*
 * IndexOptions.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;

import java.util.Collections;
import java.util.Map;

/**
 * The standard options for use with {@link Index}.
 *
 * An option key is just a string, so that new ones can be defined outside the Record Layer core.
 *
 * @see Index#getOptions
 */
@API(API.Status.UNSTABLE)
public class IndexOptions {

    /**
     * No options.
     *
     * The default for a new {@link Index}.
     */
    public static final Map<String, String> EMPTY_OPTIONS = Collections.emptyMap();

    /**
     * If {@code "true"}, index throws {@link com.apple.foundationdb.record.RecordIndexUniquenessViolation} on attempts
     * to store duplicate values.
     * <p>
     *     Unlike most index modifications, it is safe to change this option from {@code true} to {@code false}
     *     without rebuilding the index, and thus you can change this without changing the {@code lastModifiedVersion}.
     *     Changing this from {@code false} to {@code true} requires increasing the {@code lastModifiedVersion}.
     *     If you do change the {@code lastModifiedVersion} it will require a rebuild of the index. This functionality
     *     is only usable by indexes maintained by {@link IndexMaintainer}s that have a proper implementation of
     *     {@link IndexMaintainer#clearUniquenessViolations()}.
     * </p>
     */
    public static final String UNIQUE_OPTION = "unique";
    /**
     * Options to set to enable {@link #UNIQUE_OPTION}.
     */
    public static final Map<String, String> UNIQUE_OPTIONS = Collections.singletonMap(UNIQUE_OPTION, Boolean.TRUE.toString());

    /**
     * If {@code "false"}, the index is not considered for use in queries, even if enabled, unless requested explicitly.
     *
     * @see com.apple.foundationdb.record.query.RecordQuery.Builder#setAllowedIndexes
     */
    public static final String ALLOWED_FOR_QUERY_OPTION = "allowedForQuery";
    /**
     * Options to set to disable {@link #ALLOWED_FOR_QUERY_OPTION}.
     */
    public static final Map<String, String> NOT_ALLOWED_FOR_QUERY_OPTIONS = Collections.singletonMap(ALLOWED_FOR_QUERY_OPTION, Boolean.FALSE.toString());

    /**
     * Option indicating one (or more) indexes that replace this index. If this index is replaced by multiple indexes,
     * then multiple options can be specified by setting multiple options prefixed by this value. The value of each
     * option should be the name of another index in the meta-data. If for a given
     * {@linkplain com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore record store}, every index so
     * specified has been built, then the index with this option will have its data deleted and will be marked as
     * {@linkplain com.apple.foundationdb.record.IndexState#DISABLED disabled}.
     *
     * <p>
     * Every index in the list should point to another index in the meta-data. Additionally, the graph of replacement
     * indexes should be acyclic, i.e., indexes cannot be marked as replacing themselves either directly or
     * transitively. If either requirement is not met, then the index will fail {@linkplain MetaDataValidator
     * meta-data validation}
     * </p>
     *
     * <p>
     * This option is useful as it allows the user to replace one index with another without needing to remove the
     * older index until the newer index has fully rolled out. For example, suppose there is an index {@code a} that
     * is defined on a single field, but the administrator realizes that as all queries have predicates on at least two,
     * and so wants to replace the index with two indexes, {@code a,b} and {@code a,c}. However, until those new indexes
     * have been built, it is preferable to serve queries using the {@code a} index rather than to fallback to a costly
     * full record store scan. So the options {@code {"replacedBy1": "a,b", "replacedBy2": "a,c"}} can be set on the
     * index. Until both indexes have been built, queries can continue to use index {@code a}. Once both have been
     * built, though, the old index will be automatically deleted, freeing up space.
     * </p>
     *
     * <p>
     * Note that different record stores with identical
     * {@link com.apple.foundationdb.record.RecordMetaData RecordMetaData} values may have different index states, i.e.,
     * an index may be built for some record stores but not others. Setting this option will disable the index only on
     * each store that meets the necessary criteria.
     * </p>
     *
     * @see Index#getReplacedByIndexNames()
     */
    public static final String REPLACED_BY_OPTION_PREFIX = "replacedBy";

    /**
     * The name of the {@link com.apple.foundationdb.record.provider.common.text.TextTokenizer} to use with a {@link IndexTypes#TEXT} index.
     */
    public static final String TEXT_TOKENIZER_NAME_OPTION = "textTokenizerName";
    /**
     * The version of the {@link com.apple.foundationdb.record.provider.common.text.TextTokenizer} to use with a {@link IndexTypes#TEXT} index.
     */
    public static final String TEXT_TOKENIZER_VERSION_OPTION = "textTokenizerVersion";
    /**
     * The minimum size of ngram tokenizer, when using ngram analyzer.
     */
    public static final String TEXT_TOKEN_MIN_SIZE = "textTokenMinSize";
    /**
     * The maximum size of ngram tokenizer, when using ngram analyzer.
     */
    public static final String TEXT_TOKEN_MAX_SIZE = "textTokenMaxSize";
    /**
     * If {@code "true"}, a {@link IndexTypes#TEXT} index will add a conflict range for the whole index to keep the commit size down at the expense of more conflicts.
     */
    @API(API.Status.EXPERIMENTAL)
    public static final String TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION = "textAddAggressiveConflictRanges";
    /**
     * If {@code "true"}, a {@link IndexTypes#TEXT} index will not store position numbers for tokens.
     *
     * It will only be possible to determine that an indexed field contains the token someplace.
     */
    public static final String TEXT_OMIT_POSITIONS_OPTION = "textOmitPositions";

    /**
     * The number of levels in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@link com.apple.foundationdb.async.RankedSet#DEFAULT_LEVELS} = {@value com.apple.foundationdb.async.RankedSet#DEFAULT_LEVELS}.
     */
    public static final String RANK_NLEVELS = "rankNLevels";

    /**
     * The hash function to use in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@link com.apple.foundationdb.async.RankedSet#DEFAULT_HASH_FUNCTION}.
     */
    public static final String RANK_HASH_FUNCTION = "rankHashFunction";

    /**
     * Whether duplicate keys count separtely in the {@link IndexTypes#RANK} skip list {@link com.apple.foundationdb.async.RankedSet}.
     *
     * The default is {@code false}.
     */
    public static final String RANK_COUNT_DUPLICATES = "rankCountDuplicates";

    /**
     * Size of each position bitmap for {@link IndexTypes#BITMAP_VALUE} indexes.
     *
     * The default is {@code 10,000}.
     */
    public static final String BITMAP_VALUE_ENTRY_SIZE_OPTION = "bitmapValueEntrySize";

    /**
     * Whether to remove index entry for {@link IndexTypes#COUNT} type indexes when they decrement to zero.
     *
     * This makes the existence of zero-valued entries in the index in the face of updates and deletes
     * closer to what it would be if the index were rebuilt, but still not always the same.
     * In particular,<ul>
     *   <li>A {@code SUM} index will not have entries for groups all of whose indexed values are zero.</li>
     *   <li>Changing the option for an existing index from {@code false} to {@code true} does not clear any entries.</li>
     * </ul>
     */
    public static final String CLEAR_WHEN_ZERO = "clearWhenZero";

    /**
     * Size of the portion of the grouping keys enumerated after the extrema by {@link IndexTypes#PERMUTED_MIN} and {@link IndexTypes#PERMUTED_MAX} indexes.
     */
    public static final String PERMUTED_SIZE_OPTION = "permutedSize";

    /**
     * Minimum number of slots in a node of an R-tree (except for the root node for which that minimum number may be as
     * low as {@code 0}. See {@link  RTree#DEFAULT_S} for suggestions on how to set this
     * option as well as {@link #RTREE_MAX_M}. Be aware that the following inequality must hold
     * {@code MAX_M / MIN_M >= (S + 1) / S}.
     */
    public static final String RTREE_MIN_M = "rtreeMinimumM";

    /**
     * Maximum number of slots in a node of an R-tree. See {@link  RTree#DEFAULT_S}
     * for suggestions on how to set this option.  Be aware that the following inequality must hold
     * {@code MAX_M / MIN_M >= (S + 1) / S}.
     */
    public static final String RTREE_MAX_M = "rtreeMaximumM";

    /**
     * The R-tree magic split number. The insert code path always considers {@code S} siblings for overflow handling as
     * well as perform {@code S} to {@code S + 1} sibling splits (default 2-to-3 splits) and {@code S + 1} to {@code S}
     * sibling fuses.
     */
    public static final String RTREE_SPLIT_S = "rtreeSplitS";

    /**
     * The R-tree storage format. Available options are {@code BY_SLOT}
     * (see {@link RTree.Storage#BY_SLOT}) and {@code BY_NODE}
     * (see {@link RTree.Storage#BY_NODE}).
     */
    public static final String RTREE_STORAGE = "rtreeStorage";

    /**
     * Option to indicate whether the R-tree stores Hilbert values of objects together with the point or not. Hilbert
     * values can always be recomputed, however, recomputing them incurs a CPU cost.
     */
    public static final String RTREE_STORE_HILBERT_VALUES = "rtreeStoreHilbertValues";

    /**
     * Option to indicate whether the R-tree manages and uses a secondary index to quickly find the update path.
     */
    public static final String RTREE_USE_NODE_SLOT_INDEX = "rtreeUseNodeSlotIndex";

    /**
     * HNSW-only: The metric that is used to determine distances between vectors. The default metric is
     * {@link Config#DEFAULT_METRIC}. See {@link Config#getMetric()}.
     */
    public static final String HNSW_METRIC = "hnswMetric";

    /**
     * HNSW-only: The number of dimensions used. All vectors must have exactly this number of dimensions. This option
     * must be set when interacting with a vector index as it there is no default.
     * @see Config#getNumDimensions()
     */
    public static final String HNSW_NUM_DIMENSIONS = "hnswNumDimensions";

    /**
     * HNSW-only: Indicator if all layers except layer {@code 0} use inlining. If inlining is used, each node is
     * persisted as a key/value pair per neighbor which includes the vectors of the neighbors but not for itself. If
     * inlining is not used, each node is persisted as exactly one key/value pair per node which stores its own vector
     * but specifically excludes the vectors of the neighbors. The default value is set to
     * {@link Config#DEFAULT_USE_INLINING}.
     * @see Config#isUseInlining()
     */
    public static final String HNSW_USE_INLINING = "hnswUseInlining";

    /**
     * HNSW-only: This option (named {@code M} by the HNSW paper) is the connectivity value for all nodes stored on
     * any layer. While by no means enforced or even enforceable, we strive to create and maintain exactly {@code m}
     * neighbors for a node. Due to insert/delete operations it is possible that the actual number of neighbors a node
     * references is not exactly {@code m} at any given time. The default value is set to {@link Config#DEFAULT_M}.
     * @see Config#getM()
     */
    public static final String HNSW_M = "hnswM";

    /**
     * HNSW-only: This attribute (named {@code M_max} by the HNSW paper) is the maximum connectivity value for nodes
     * stored on a layer greater than {@code 0}. A node can never have more that {@code mMax} neighbors. That means that
     * neighbors of a node are pruned if the actual number of neighbors would otherwise exceed {@code mMax}. Note that
     * this option must be greater than or equal to {@link #HNSW_M}. The default value is set to
     * {@link Config#DEFAULT_M_MAX}.
     * @see Config#getMMax()
     */
    public static final String HNSW_M_MAX = "hnswMMax";

    /**
     * HNSW-only: This option (named {@code M_max0} by the HNSW paper) is the maximum connectivity value for nodes
     * stored on layer {@code 0}. We will never create more that {@code mMax0} neighbors for a node that is stored on
     * that layer. That means that we even prune the neighbors of a node if the actual number of neighbors would
     * otherwise exceed {@code mMax0}. Note that this option must be greater than or equal to {@link #HNSW_M_MAX}.
     * The default value is set to {@link Config#DEFAULT_M_MAX_0}.
     * @see Config#getMMax0()
     */
    public static final String HNSW_M_MAX_0 = "hnswMMax0";

    /**
     * HNSW-only: Maximum size of the search queues (one independent queue per layer) that are used during the insertion
     * of a new node. If {@code HNSW_EF_CONSTRUCTION} is set to {@code 1}, the search naturally follows a greedy
     * approach (monotonous descent), whereas a high number for {@code HNSW_EF_CONSTRUCTION} allows for a more nuanced
     * search that can tolerate (false) local minima. The default value is set to {@link Config#DEFAULT_EF_CONSTRUCTION}.
     * @see Config#getEfConstruction()
     */
    public static final String HNSW_EF_CONSTRUCTION = "hnswEfConstruction";

    /**
     * HNSW-only: Maximum number of candidate nodes that are considered when a HNSW layer is locally repaired as part of
     * a delete operation. A smaller number causes the delete operation to create a smaller set of candidate nodes
     * which improves repair performance but decreases repair quality; a higher number results in qualitatively
     * better repairs at the expense of slower performance.
     * The default value is set to {@link Config#DEFAULT_EF_REPAIR}.
     * @see Config#getEfRepair()
     */
    public static final String HNSW_EF_REPAIR = "hnswEfRepair";

    /**
     * HNSW-only: Indicator to signal if, during the insertion of a node, the set of nearest neighbors of that node is
     * to be extended by the actual neighbors of those neighbors to form a set of candidates that the new node may be
     * connected to during the insert operation. The default value is set to {@link Config#DEFAULT_EXTEND_CANDIDATES}.
     * @see Config#isExtendCandidates()
     */
    public static final String HNSW_EXTEND_CANDIDATES = "hnswExtendCandidates";

    /**
     * HNSW-only: Indicator to signal if, during the insertion of a node, candidates that have been discarded due to not
     * satisfying the select-neighbor heuristic may get added back in to pad the set of neighbors if the new node would
     * otherwise have too few neighbors (see {@link Config#getM()}). The default value is set to
     * {@link Config#DEFAULT_KEEP_PRUNED_CONNECTIONS}.
     * @see Config#isKeepPrunedConnections()
     */
    public static final String HNSW_KEEP_PRUNED_CONNECTIONS = "hnswKeepPrunedConnections";

    /**
     * HNSW-only: If sampling is necessary (currently iff {@link #HNSW_USE_RABITQ} is {@code "true"}), this option
     * represents the probability of a vector being inserted to also be written into the samples subspace of the hnsw
     * structure. The vectors in that subspace are continuously aggregated until a total {@link #HNSW_STATS_THRESHOLD}
     * has been reached. The default value is set to {@link Config#DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY}. See
     * @see Config#getSampleVectorStatsProbability()
     */
    public static final String HNSW_SAMPLE_VECTOR_STATS_PROBABILITY = "hnswSampleVectorStatsProbability";

    /**
     * HNSW-only: If sampling is necessary (currently iff {@link #HNSW_USE_RABITQ} is {@code "true"}), this option
     * represents the probability of the samples subspace to be further aggregated (rolled-up) when a new vector is
     * inserted. The vectors in that subspace are continuously aggregated until a total
     * {@link #HNSW_STATS_THRESHOLD} has been reached. The default value is set to
     * {@link Config#DEFAULT_MAINTAIN_STATS_PROBABILITY}.
     * @see Config#getMaintainStatsProbability()
     */
    public static final String HNSW_MAINTAIN_STATS_PROBABILITY = "hnswMaintainStatsProbability";

    /**
     * HNSW-only: If sampling is necessary (currently iff {@link #HNSW_USE_RABITQ} is {@code "true"}), this option
     * represents the threshold (being a number of vectors) that when reached causes the stats maintenance logic to
     * compute the actual statistics (currently the centroid of the vectors that have been inserted to far). The result
     * is then inserted into the access info subspace of the index. The default value is set to
     * {@link Config#DEFAULT_STATS_THRESHOLD}.
     * @see Config#getStatsThreshold()
     */
    public static final String HNSW_STATS_THRESHOLD = "hnswStatsThreshold";

    /**
     * HNSW-only: Indicator if we should RaBitQ quantization. See {@link com.apple.foundationdb.rabitq.RaBitQuantizer}
     * for more details. The default value is set to {@link Config#DEFAULT_USE_RABITQ}.
     * @see Config#isUseRaBitQ()
     */
    public static final String HNSW_USE_RABITQ = "hnswUseRaBitQ";

    /**
     * HNSW-only: Number of bits per dimensions iff {@link #HNSW_USE_RABITQ} is set to {@code "true"}, ignored
     * otherwise. If RaBitQ encoding is used, a vector is stored using roughly
     * {@code 25 + numDimensions * (numExBits + 1) / 8} bytes. The default value is set to
     * {@link Config#DEFAULT_RABITQ_NUM_EX_BITS}.
     * @see Config#getRaBitQNumExBits()
     */
    public static final String HNSW_RABITQ_NUM_EX_BITS = "hnswRaBitQNumExBits";

    /**
     * HNSW-only: Maximum number of concurrent node fetches during search and modification operations. The default value
     * is set to {@link Config#DEFAULT_MAX_NUM_CONCURRENT_NODE_FETCHES}.
     * @see Config#getMaxNumConcurrentNodeFetches()
     */
    public static final String HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES = "hnswMaxNumConcurrentNodeFetches";

    /**
     * HNSW-only: Maximum number of concurrent neighborhood fetches during modification operations when the neighbors
     * are pruned. The default value is set to {@link Config#DEFAULT_MAX_NUM_CONCURRENT_NEIGHBOR_FETCHES}.
     * @see Config#getMaxNumConcurrentNeighborhoodFetches()
     */
    public static final String HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES = "hnswMaxNumConcurrentNeighborhoodFetches";

    /**
     * HNSW-only: Maximum number of delete operations that can run concurrently in separate layers during the deletion
     * of a record. The default value is set to {@link Config#DEFAULT_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER}.
     * @see Config#getMaxNumConcurrentDeleteFromLayer()
     */
    public static final String HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER = "hnswMaxNumConcurrentDeleteFromLayer";

    private IndexOptions() {
    }
}

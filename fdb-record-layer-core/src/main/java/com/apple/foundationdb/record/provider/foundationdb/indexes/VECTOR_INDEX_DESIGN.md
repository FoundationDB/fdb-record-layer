# Vector Index and HNSW Design Document
## FoundationDB Record Layer

---

## Summary

This document provides a comprehensive design analysis of the vector indexing infrastructure in the
FoundationDB Record Layer, focusing on the existing HNSW implementation. The architecture supports advanced features
including RaBitQ quantization, top-N approximate nearest neighbor searches, order-by distance queries.

---

## 1. Architecture

### 1.1 Overview

```
┌─────────────────────────────────────────┐
│           fdb-record-layer-core         │
│ ┌─────────────────────────────────────┐ │
│ │        VectorIndexMaintainer        │ │
│ │  • BY_DISTANCE scan type            │ │
│ │  • VectorIndexScanBounds            │ │
│ │  • Query planner integration        │ │
│ └─────────────────────────────────────┘ │
└─────────────┬───────────────────────────┘
              │ depends on
┌─────────────▼───────────────────────────┐
│           fdb-extensions                │
│ ┌─────────────────────────────────────┐ │
│ │      HNSW                           │ │
│ │  • Top-N ANN searches               │ │
│ │  • Order-By scans                   │ │
│ │  • RaBitQ quantization              │ │
│ └─────────────────────────────────────┘ │
│ ┌─────────────────────────────────────┐ │
│ │        Supporting Libraries         │ │
│ │  • linear (vectors, matrices)       │ │
│ │  • rabitq (compression)             │ │
│ └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

---

## 2. Detailed Component Analysis

### 2.1 HNSW Implementation

#### **Core Algorithm Components**

**Key Features**:
- **Hierarchical Structure**: Multi-layer graph with exponentially decreasing layer sizes
- **Configurable Parameters**: M=16, efConstruction=200
- **Storage Adaptors**: `StorageAdapter` abstraction for FoundationDB integration
- **Node Types**: `InliningNode` vs `CompactNode` for performance optimization
- **Async Operations**: Non-blocking insert/delete/search

**Storage Layout** (in FoundationDB):
```
Index Subspace:
├── DATA (0x00) Node Storage
│   ├── Layer 0: Full vector data + neighbor links
│   ├── Layer N: Sparse nodes with long-range connections
│   └── Entry Point: Top-layer entry node reference
├── ACCESS_INFO (0x01): HNSW metadata, current quantization configuration,
|                       currently employed coordinate system, entry node
└── STATS (0x02): Optional sampling data for adaptive parameters
```

#### Algorithm

##### Search

The HNSW search algorithm implements a two-phase approach that leverages the hierarchical structure to achieve logarithmic search complexity. The search begins by identifying the entry point from the ACCESS_INFO subspace, which contains the topmost node reference in the highest layer of the graph. The algorithm then performs a greedy search starting from this entry point, descending through each layer of the hierarchy until reaching the base layer (layer 0) where all vectors reside. During the descent through upper layers, the search maintains a single best candidate at each level, using this candidate as the entry point for the subsequent lower layer. This greedy traversal through the sparse upper layers allows the algorithm to quickly navigate to the appropriate region of the vector space.

Upon reaching layer 0, the search algorithm switches to an exhaustive beam search that maintains a dynamic candidate list of size `efSearch`. This candidate list acts as a priority queue that tracks the most promising nodes discovered during traversal. The beam search explores the dense connectivity of layer 0 by following neighbor connections and continuously updating the candidate list with closer vectors. The `efSearch` parameter directly controls the trade-off between search accuracy and computational cost, where larger values produce higher recall at the expense of increased node fetches and distance computations. The algorithm leverages asynchronous operations throughout the search process, allowing concurrent fetching of multiple nodes when the storage adapter supports it, with concurrency limits controlled by `maxNumConcurrentNodeFetches`.

The search process employs distance estimation techniques when RaBitQ quantization is enabled, allowing approximate distance computations to be performed directly in the compressed vector space before fetching full vector data. This optimization significantly reduces I/O overhead when scanning large numbers of candidates. The final search results are sorted by distance and truncated to the requested number of nearest neighbors (k), with the option to include full vector data in the results controlled by the `includeVectors` parameter. All distance computations respect the configured metric (Euclidean, cosine, dot product, etc.) and coordinate transformations applied through the `StorageTransform` component.

##### Insert

The HNSW insertion algorithm implements a multi-stage process that integrates new vectors into the hierarchical graph structure while maintaining optimal connectivity and search performance. The insertion begins with layer assignment, where the algorithm uses a probabilistic method based on an exponential decay distribution with parameter `ml = 1/ln(2.0)` to determine the highest layer where the new node will be placed. This probabilistic assignment ensures that higher layers contain exponentially fewer nodes, creating the logarithmic search complexity characteristic of HNSW. The new node will be present on all layers from 0 up to its assigned top layer, with layer 0 guaranteed to contain the node since it holds all vectors in the index.

The insertion process then proceeds through a layer-by-layer search and connection phase, starting from the highest existing layer and working downward to layer 0. For layers above the new node's top layer, the algorithm performs greedy searches to find entry points that will guide the search in lower layers. These greedy searches maintain single best candidates per layer and serve to navigate quickly through the sparse upper layers without creating connections. Once the algorithm reaches layers where the new node will actually be inserted, it switches to an exhaustive search using the `efConstruction` parameter to control the size of the dynamic candidate list. This larger candidate pool during construction allows the algorithm to explore more thoroughly and create higher-quality connections compared to the search-time `efSearch` parameter.

At each layer where the new node is inserted, the algorithm performs several critical operations to maintain graph integrity. First, it conducts an extensive search from the entry points found in the layer above, building a candidate list of potential neighbors sized according to `efConstruction`. From this candidate pool, the algorithm selects the optimal subset of neighbors using distance-based heuristics, respecting the connectivity limits defined by parameters `m`, `mMax`, and `mMax0`. The selection process may optionally extend the candidate set by including neighbors of neighbors when `extendCandidates` is enabled, and may retain initially pruned connections when `keepPrunedConnections` is enabled to ensure minimum connectivity requirements. After selecting the new node's neighbors, the algorithm creates bidirectional connections by updating both the new node's neighbor list and reciprocally updating each selected neighbor to include the new node.

The insertion process includes connection maintenance to prevent graph degradation over time. When adding the new node causes existing neighbors to exceed their maximum connection limits (`mMax` or `mMax0`), the algorithm prunes excess connections using selection heuristics that prioritize maintaining graph quality. All node updates are persisted atomically within the provided transaction context, ensuring consistency even in the presence of concurrent operations. The algorithm also handles special cases such as inserting the first node into an empty index or inserting a node whose layer exceeds the current maximum layer, which requires updating the entry point stored in ACCESS_INFO. When RaBitQ quantization is enabled, the insertion process probabilistically samples vectors for statistical analysis using `sampleVectorStatsProbability` and triggers periodic maintenance of quantization parameters when the sample count reaches `statsThreshold`.

##### Delete/Repair

The HNSW deletion algorithm implements a graph repair mechanism that maintains structural integrity and search performance after removing nodes from the hierarchical graph. The deletion process begins with layer discovery, where the algorithm fetches the target node from storage to determine which layers contain the node and to retrieve its complete neighbor lists across all layers. This initial phase also identifies the node's top layer, which is crucial for determining whether the deletion might affect the global entry point stored in ACCESS_INFO. The algorithm then determines the node's layer assignment using the same probabilistic method employed during insertion, ensuring consistent layer membership across operations and enabling proper cleanup of all node instances.

The core of the deletion algorithm focuses on graph repair, which addresses the connectivity gaps created when removing a node and its associated edges. For each layer containing the target node, the algorithm collects all directly connected neighbors (primary neighbors) and extends this set by including the neighbors of those neighbors (secondary neighbors) to form a comprehensive candidate pool for repair operations. This candidate expansion ensures that the repair process has sufficient options to create high-quality replacement connections that maintain graph traversability. The algorithm employs sampling controlled by the `efRepair` parameter to manage the size of the candidate pool, balancing repair quality against computational overhead by limiting the number of candidates considered during the repair process.

The repair mechanism operates by systematically reconnecting the orphaned neighbors of the deleted node through a sophisticated neighbor selection process. For each primary neighbor of the deleted node, the algorithm computes distances to all candidates in the repair pool and selects optimal new connections using the same distance-based heuristics employed during insertion. This process respects the connectivity constraints defined by `m`, `mMax`, and `mMax0` parameters, ensuring that repaired connections maintain the graph's structural properties. The algorithm tracks all modifications through a `NeighborsChangeSet` mechanism that batches updates for efficient persistence, allowing multiple neighbors to be repaired concurrently up to the limit defined by `maxNumConcurrentNeighborhoodFetches`.

Throughout the repair process, the algorithm maintains careful coordination between the removal of the target node and the creation of replacement connections. The actual node deletion occurs only after all repair operations have been computed and prepared for persistence. When the deleted node was serving as the global entry point (the topmost node in the highest layer), the algorithm must identify and update the new entry point by finding the highest-layer node with the maximum number of layers. All repair operations are performed asynchronously with appropriate concurrency controls, and the entire deletion process completes within a single transaction context to maintain consistency. The algorithm also includes extensive logging and verification to ensure proper graph maintenance and to facilitate debugging of complex multi-layer repair scenarios.

#### **Configuration Options**

The HNSW implementation provides extensive configuration through `IndexOptions` constants. Each parameter allows fine-tuning of performance, storage efficiency, and search quality characteristics.

| HNSW Parameter | IndexOptions Constant | Detailed Explanation |
|---|---|---|
| **Core Parameters** |||
| numDimensions | `HNSW_NUM_DIMENSIONS` | **Required parameter** specifying the exact number of dimensions for all vectors in the index. Every vector inserted must have precisely this dimensionality. There is no default value and this option must be explicitly set during index creation. This parameter determines the size of vector storage and affects all distance computations. |
| metric | `HNSW_METRIC` | The distance metric used for vector similarity calculations. Supported options include `EUCLIDEAN` (L2 distance), `DOT_PRODUCT` (inner product), `COSINE` (cosine similarity), and `EUCLIDEAN_SQUARED` (squared L2 distance). The default is `EUCLIDEAN`. The choice significantly impacts search behavior and should match the semantic meaning of your vector space. |
| **Graph Connectivity Parameters** |||
| m | `HNSW_M` | The target number of bidirectional connections (edges) that each node maintains in the graph structure. This is the fundamental connectivity parameter that controls the graph density. Higher values create more connected graphs with better search recall but increased storage overhead and slower insertion performance. The default value is 16, which provides a good balance for most applications. |
| mMax | `HNSW_M_MAX` | The maximum number of connections allowed for nodes on layers greater than 0 (i.e., all layers except the bottom layer). When the number of connections would exceed this limit during insertion, the algorithm prunes connections using a selection heuristic. This parameter must be greater than or equal to `HNSW_M`. The default value equals `HNSW_M` (16). |
| mMax0 | `HNSW_M_MAX_0` | The maximum number of connections allowed specifically for nodes on layer 0 (the bottom layer containing all vectors). Since layer 0 is the most densely connected layer and affects search quality most directly, it typically allows more connections than higher layers. This parameter must be greater than or equal to `HNSW_M_MAX`. The default value is 32 (twice `HNSW_M`). |
| **Construction Quality Parameters** |||
| efConstruction | `HNSW_EF_CONSTRUCTION` | The size of the dynamic candidate list maintained during index construction when inserting new vectors. This parameter controls the trade-off between build time and index quality. Larger values result in better graph connectivity and higher search recall at the cost of slower insertion performance. A value of 1 forces greedy insertion, while higher values allow the algorithm to escape local minima during construction. The default value is 200. |
| efRepair | `HNSW_EF_REPAIR` | The number of candidate nodes considered when repairing graph connectivity after a node deletion. During deletion, the algorithm must reconnect neighboring nodes to maintain graph integrity. Smaller values improve deletion performance but may reduce search quality, while larger values provide better repair quality at increased computational cost. The default value is 64. |
| **Algorithm Optimization** |||
| extendCandidates | `HNSW_EXTEND_CANDIDATES` | When enabled, the insertion algorithm extends the candidate set by including the neighbors of the initially found nearest neighbors. This creates a larger pool of potential connections, potentially improving graph quality but increasing insertion computational cost. The feature helps create more robust connections in dense regions of the vector space. The default value is `false`. |
| keepPrunedConnections | `HNSW_KEEP_PRUNED_CONNECTIONS` | When enabled, the algorithm may retain connections that were initially pruned by the selection heuristic if the node would otherwise have fewer than `HNSW_M` connections. This helps maintain minimum connectivity requirements and can improve search recall, especially in sparse regions of the vector space. The default value is `false`. |
| **Storage Optimization** |||
| useInlining | `HNSW_USE_INLINING` | Controls the storage format for graph nodes in layers above 0. When enabled (inlining), each neighbor connection is stored as a separate key-value pair that includes the neighbor's vector data but excludes the node's own vector. When disabled (compact), each node is stored as a single key-value pair containing its own vector but excluding neighbor vectors. Inlining can improve cache locality for traversal-heavy workloads but increases storage overhead. The default value is `false` (compact storage). |
| **Vector Quantization** |||
| useRaBitQ | `HNSW_USE_RABITQ` | Enables RaBitQ (Rapid Bit Quantization) compression for vector storage. When enabled, vectors are compressed using a sophisticated quantization scheme that reduces storage by 8-10x while maintaining search accuracy. This feature is particularly beneficial for high-dimensional vectors or storage-constrained environments. Quantized vectors support fast approximate distance computations directly in compressed space. The default value is `false`. |
| raBitQNumExBits | `HNSW_RABITQ_NUM_EX_BITS` | Specifies the number of bits per dimension used in RaBitQ quantization (only relevant when `HNSW_USE_RABITQ` is enabled). The storage requirement per vector is approximately `25 + numDimensions * (numExBits + 1) / 8` bytes. Lower values provide higher compression but reduced accuracy, while higher values maintain better accuracy with less compression. The default value is 4 bits per dimension. |
| **Statistics and Adaptive Behavior** |||
| sampleVectorStatsProbability | `HNSW_SAMPLE_VECTOR_STATS_PROBABILITY` | The probability that each inserted vector is sampled for statistical analysis (only used when RaBitQ quantization is enabled). Sampled vectors are stored in a dedicated subspace and used to compute aggregate statistics like centroids for improving quantization quality. Higher probabilities provide better statistics but increase storage overhead. The default value is 0.5 (50% sampling rate). |
| maintainStatsProbability | `HNSW_MAINTAIN_STATS_PROBABILITY` | The probability that the statistics maintenance process runs when a new vector is inserted (only used with RaBitQ). This process aggregates sampled vectors and updates the quantization parameters based on the evolving data distribution. The maintenance runs when the number of samples reaches `HNSW_STATS_THRESHOLD`. The default value is 0.05 (5% probability). |
| statsThreshold | `HNSW_STATS_THRESHOLD` | The number of sampled vectors that must accumulate before the statistics maintenance process computes updated aggregate statistics (only relevant with RaBitQ). When this threshold is reached, the system computes new centroids and updates quantization parameters stored in the index's access info subspace. The default value is 1000 vectors. |
| **Concurrency Control** |||
| maxNumConcurrentNodeFetches | `HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES` | Controls the maximum number of graph nodes that can be fetched concurrently from FoundationDB during search and modification operations. Higher values can improve throughput for operations that require many node reads, but may increase FoundationDB load and memory usage. This parameter should be tuned based on your FoundationDB cluster capacity and expected query patterns. The default value is 16. |
| maxNumConcurrentNeighborhoodFetches | `HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES` | Limits the number of concurrent neighborhood fetches during graph modification operations, particularly when pruning connections during insertion. This parameter affects the parallelism of complex graph updates that require reading multiple neighborhoods simultaneously. The default value is 10. |
| maxNumConcurrentDeleteFromLayer | `HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER` | Controls how many layer-specific deletion operations can run in parallel when removing a vector from the index. Since HNSW deletion requires updating multiple layers of the graph hierarchy, this parameter allows parallelization across layers while preventing excessive concurrent load. The default value is 2. |

### 2.3 Integration with Record Layer

#### **VectorIndexMaintainer**

**Key Features**:
- **Extends StandardIndexMaintainer**: Inherits index lifecycle management
- **BY_DISTANCE Scanning**: Implements k-NN search via `IndexScanType.BY_DISTANCE`
- **Partitioning Support**: Prefix-based partitioning
- **Async Lock-based Concurrency**: Write locks per partition prevent conflicts
- **Instrumentation**: Detailed metrics via `OnReadListener` and `OnWriteListener`

**Scanning Architecture**:
```java
public RecordCursor<IndexEntry> scan(IndexScanBounds scanBounds, ...) {
    if (prefixSize > 0) {
        // Multi-partition: FlatMap over prefixes
        return RecordCursor.flatMapPipelined(
            prefixSkipScan(...),  // Discover unique prefixes
            (prefixTuple, continuation) ->
                scanSinglePartition(prefixTuple, ...)  // Search each partition
        );
    } else {
        // Single partition: Direct HNSW search
        return scanSinglePartition(null, indexSubspace, ...);
    }
}
```

**Update Process**:
```java
protected CompletableFuture<Void> updateIndexKeys(...) {
    final byte[] vectorBytes = indexEntry.getValue().getBytes(0);
    if (vectorBytes == null) return AsyncUtil.DONE;  // Skip null vectors

    return state.context.doWithWriteLock(lockIdentifier, () -> {
        final HNSW hnsw = new HNSW(subspace, executor, config, ...);
        if (remove) {
            return hnsw.delete(transaction, primaryKey);
        } else {
            return hnsw.insert(transaction, primaryKey,
                RealVector.fromBytes(vectorBytes), null);
        }
    });
}
```

---

## 3. Advanced Features

### 3.1 Vector Compression (RaBitQ)

**Benefits**:
- **Storage Reduction**: 8-10x compression with minimal accuracy loss
- **Fast Distance Estimation**: Direct computation in compressed space

**Implementation**:
- **Quantizer Interface**: Pluggable compression strategies
- **Encoded Vectors**: `EncodedRealVector` for compressed storage
- **Estimation**: `RaBitEstimator` for fast approximate distances

### 3.2 Linear Algebra Foundation

**Core Types**:
- **RealVector**: Base interface with precision variants
  - `DoubleRealVector` (64-bit)
  - `FloatRealVector` (32-bit)
  - `HalfRealVector` (16-bit)
- **Distance Metrics**: Euclidean, cosine, dot product
- **Matrix Operations**: QR decomposition, affine transformations
- **Coordinate Transforms**: Fast Hadamard Transform rotation

---

## 4. Performance Characteristics

### 4.1 HNSW Performance

**Time Complexity**:
- **Insert**: `O(log N × M × efConstruction)`
- **Search**: `O(log N × efSearch)`
- **Delete**: `O(log N × M × efRepair)`

**Space Complexity**:
- **Storage**: `O(N × M × layers)` for graph structure
- **Memory**: `O(efSearch)` for beam search
- **Vectors**: `O(N × dimensions)`

**Tuning Parameters**:
- **M**: Connectivity; determines the number of neighbors a node have (roughly) Higher M = better accuracy, more storage
- **efConstruction**: Higher efConstruction = better graph quality, slower build
- **efSearch**: Higher efSearch = better recall, slower search

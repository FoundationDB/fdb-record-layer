/*
 * NodeInfo.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.explain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * Explanatory information about the Record Layer's plan operators and storage objects.
 *
 * Note that it is possible that users of the record layer may want to amend this set of infos. Therefore it is a
 * class with statics rather than a sealed enumeration.
 */
public class NodeInfo {
    public static final NodeInfo BASE_DATA = new NodeInfo(
            "BaseData",
            NodeIcon.BASE_DATA,
            "Primary Storage",
            "Primary storage stores the data of all records.");
    public static final NodeInfo INDEX_DATA = new NodeInfo(
            "IndexData",
            NodeIcon.INDEX_DATA,
            "Index",
            "An index is a data structure that is created based on a user-supplied ordering that enables index scans.");
    public static final NodeInfo TEMPORARY_BUFFER_DATA = new NodeInfo(
            "TempBufferData",
            NodeIcon.IN_MEMORY_TEMPORARY_DATA,
            "Temporary Buffer",
            "A temporary buffer is a temporary, in-memory structure storing up to a fixed number of records.");
    public static final NodeInfo VALUES_DATA = new NodeInfo(
            "ValuesData",
            NodeIcon.IN_MEMORY_TEMPORARY_DATA,
            "Values",
            "A constant list of values.");
    public static final NodeInfo COMPOSED_BITMAP_OPERATOR = new NodeInfo(
            "ComposedBitmapOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Composed Bitmap",
            "A set of Boolean conditions on BITMAP_VALUE indexed bitmaps which are combined into a single bitmap by bitwise operations.");
    public static final NodeInfo COVERING_INDEX_SCAN_OPERATOR = new NodeInfo(
            "CoveringIndexScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Covering Index Scan",
            "A covering index scan operator uses a secondary index to quickly find records in a given search range without looking up the base records.");
    public static final NodeInfo COVERING_SPATIAL_INDEX_SCAN_OPERATOR = new NodeInfo(
            "CoveringSpatialIndexScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Covering Spatial Index Scan",
            "A covering spatial index scan operator uses a spatial index to quickly find records in the index for the given spatial predicate without looking up the base records.");
    public static final NodeInfo INDEX_SCAN_OPERATOR = new NodeInfo(
            "IndexScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Index Scan",
            "An index scan operator uses a secondary index to quickly find records in the index within a given search range.");
    public static final NodeInfo INTERSECTION_OPERATOR = new NodeInfo(
            "IntersectionOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Intersection",
            "An intersection operator processes its inputs in a common order and only returns those records that appear in all inputs.");
    public static final NodeInfo NESTED_LOOP_JOIN_OPERATOR = new NodeInfo(
            "NestedLoopJoinOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Nested Loop Join",
            "A nested loop join operator performs a relational join between its two input operands by reevaluating the right (inner) side for each left (outer) record.");
    public static final NodeInfo LOAD_BY_KEYS_OPERATOR = new NodeInfo(
            "LoadByKeysOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Load By Keys",
            "A load by keys operator loads records from the database that match the given set of primary keys.");
    public static final NodeInfo FETCH_OPERATOR = new NodeInfo(
            "FetchOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Fetch Records",
            "A fetch retrieves records from the primary key store using the provided index entries.");
    public static final NodeInfo PREDICATE_FILTER_OPERATOR = new NodeInfo(
            "PredicateFilterOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Predicate Filter",
            "A predicate filter operator processes the input and returns only those records for which the predicate expression is true.");
    public static final NodeInfo SCAN_OPERATOR = new NodeInfo(
            "ScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Scan",
            "A scan operator loads a set of records from the database that are within the given range of primary keys.");
    public static final NodeInfo SCORE_FOR_RANK_OPERATOR = new NodeInfo(
            "ScoreForRankOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Score For Rank",
            "A score for rank operator converts ranks to scores and executes its input plan with the conversion results bound in named parameters.");
    public static final NodeInfo SORT_OPERATOR = new NodeInfo(
            "SortOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Sort",
            "A sort operation reordering the stream of records according to a given expression.");
    public static final NodeInfo SPATIAL_INDEX_SCAN_OPERATOR = new NodeInfo(
            "SpatialIndexScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Spatial Index Scan",
            "A spatial index scan operator uses a spatial index to quickly find records in the index for the given spatial predicate.");
    public static final NodeInfo TABLE_FUNCTION_OPERATOR = new NodeInfo(
            "TableFunctionOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Table Function",
            "A table function operator returns a table of records each time it is evaluated with a set of arguments that are specific to each individual table function.");
    public static final NodeInfo TEXT_INDEX_SCAN_OPERATOR = new NodeInfo(
            "TextIndexScanOperator",
            NodeIcon.DATA_ACCESS_OPERATOR,
            "Text Index Scan",
            "A text index scan operator uses a text index to quickly find records in the index for the given full text predicate.");
    public static final NodeInfo TYPE_FILTER_OPERATOR = new NodeInfo(
            "TypeFilterOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Type Filter",
            "A type filter operator processes the input and returns only those records for whose type is among the given set of types.");
    public static final NodeInfo UNION_OPERATOR = new NodeInfo(
            "UnionOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Union Distinct",
            "An ordered union operator processes produces the set of all input records returned by its two or more inputs (all records are distinct). This operator needs all inputs to be compatibly ordered, and returns them in the same order.");
    public static final NodeInfo UNORDERED_DISTINCT_OPERATOR = new NodeInfo(
            "UnorderedDistinct",
            NodeIcon.COMPUTATION_OPERATOR,
            "Unordered Distinct",
            "An unordered distinct operator processes its input records and returns the set of distinct records. It does not require its input records to be ordered but does impose a memory overhead as it needs to track records it has already encountered.");
    public static final NodeInfo UNORDERED_PRIMARY_KEY_DISTINCT_OPERATOR = new NodeInfo(
            "UnorderedPrimaryKeyDistinct",
            NodeIcon.COMPUTATION_OPERATOR,
            "Unordered Primary Key Distinct",
            "An unordered primary key distinct operator processes its input records and returns the set of distinct records. It does not require its input records to be ordered but does impose a memory overhead as it needs to track primary keys of records it has already encountered.");
    public static final NodeInfo UNORDERED_UNION_OPERATOR = new NodeInfo(
            "UnorderedUnionOperator",
            NodeIcon.COMPUTATION_OPERATOR,
            "Union All",
            "A union all operator processes its two or more inputs and returns the unioned multiset of all input records (duplicates are preserved). The operator does not required its inputs to be compatible ordered.");

    private final String id;
    private final String iconId;
    private final String name;
    private final String description;

    public NodeInfo(@Nonnull final String id,
                    @Nonnull final NodeIcon nodeIcon,
                    @Nonnull final String name,
                    @Nonnull final String description) {
        this.id = id;
        this.iconId = nodeIcon.getId();
        this.name = name;
        this.description = description;
    }

    @Nonnull
    public String getId() {
        return id;
    }

    @Nonnull
    public String getIconId() {
        return iconId;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }

    @Nonnull
    public static Set<NodeInfo> getNodeInfos() {
        return ImmutableSet.of(
                BASE_DATA,
                INDEX_DATA,
                TEMPORARY_BUFFER_DATA,
                VALUES_DATA,
                COVERING_SPATIAL_INDEX_SCAN_OPERATOR,
                COVERING_INDEX_SCAN_OPERATOR,
                INDEX_SCAN_OPERATOR,
                INTERSECTION_OPERATOR,
                NESTED_LOOP_JOIN_OPERATOR,
                LOAD_BY_KEYS_OPERATOR,
                PREDICATE_FILTER_OPERATOR,
                SCAN_OPERATOR,
                SCORE_FOR_RANK_OPERATOR,
                SPATIAL_INDEX_SCAN_OPERATOR,
                TABLE_FUNCTION_OPERATOR,
                TEXT_INDEX_SCAN_OPERATOR,
                TYPE_FILTER_OPERATOR,
                UNION_OPERATOR,
                UNORDERED_DISTINCT_OPERATOR,
                UNORDERED_PRIMARY_KEY_DISTINCT_OPERATOR,
                UNORDERED_UNION_OPERATOR);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    public static Map<String, Attribute> getInfoAttributeMap(final Set<NodeInfo> nodeInfos) {
        return nodeInfos
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        NodeInfo::getId,
                        nodeInfo -> Attribute.gml(
                                ImmutableMap.of(
                                        "iconId", Attribute.gml(nodeInfo.getIconId()),
                                        "name", Attribute.gml(nodeInfo.getName()),
                                        "description", Attribute.gml(nodeInfo.getDescription())))));
    }
}

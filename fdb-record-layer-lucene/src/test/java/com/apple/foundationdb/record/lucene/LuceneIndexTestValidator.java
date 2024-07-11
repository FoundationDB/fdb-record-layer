/*
 * LuceneIndexValidator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.search.LuceneOptimizedIndexSearcher;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A utility for validating the consistency and contents of a lucene index.
 */
public class LuceneIndexTestValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexTestValidator.class);
    private final Supplier<FDBRecordContext> contextProvider;
    private final Function<FDBRecordContext, FDBRecordStore> schemaSetup;

    public LuceneIndexTestValidator(Supplier<FDBRecordContext> contextProvider, Function<FDBRecordContext, FDBRecordStore> schemaSetup) {
        this.contextProvider = contextProvider;
        this.schemaSetup = schemaSetup;
    }

    /**
     * A broad validation of the lucene index, asserting consistency, and that various operations did what they were
     * supposed to do.
     * <p>
     *     This has a lot of validation that could be added, and it would be good to be able to control whether it's
     *     expected that `mergeIndex` had been run or not; right now it assumes it has been run.
     * </p>
     * @param index the index to validate
     * @param expectedDocumentInformation a map from group to primaryKey to timestamp
     * @param universalSearch a search that will return all the documents
     * @throws IOException if there is any issue interacting with lucene
     */
    void validate(Index index, final Map<Tuple, Map<Tuple, Tuple>> expectedDocumentInformation,
                  final String universalSearch) throws IOException {
        validate(index, expectedDocumentInformation, universalSearch, false);
    }

    /**
     * A broad validation of the lucene index, asserting consistency, and that various operations did what they were
     * supposed to do.
     * <p>
     *     This has a lot of validation that could be added, and it would be good to be able to control whether it's
     *     expected that `mergeIndex` had been run or not; right now it assumes it has been run.
     * </p>
     * @param index the index to validate
     * @param expectedDocumentInformation a map from group to primaryKey to timestamp
     * @param universalSearch a search that will return all the documents
     * @param allowDuplicatePrimaryKeys if {@code true} this will allow multiple entries in the primary key segment
     * index for the same primaary key. This should only be {@code true} if you expect merges to fail.
     * @throws IOException if there is any issue interacting with lucene
     */
    void validate(Index index, final Map<Tuple, Map<Tuple, Tuple>> expectedDocumentInformation,
                  final String universalSearch, final boolean allowDuplicatePrimaryKeys) throws IOException {
        final int partitionHighWatermark = Integer.parseInt(index.getOption(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK));
        String partitionLowWaterMarkStr = index.getOption(LuceneIndexOptions.INDEX_PARTITION_LOW_WATERMARK);
        final int partitionLowWatermark = partitionLowWaterMarkStr == null ?
                                          Math.max(LucenePartitioner.DEFAULT_PARTITION_LOW_WATERMARK, 1) :
                                          Integer.parseInt(partitionLowWaterMarkStr);

        Map<Tuple, Map<Tuple, Tuple>> missingDocuments = new HashMap<>();
        expectedDocumentInformation.forEach((groupingKey, groupedIds) -> {
            missingDocuments.put(groupingKey, new HashMap<>(groupedIds));
        });
        for (final Map.Entry<Tuple, Map<Tuple, Tuple>> entry : expectedDocumentInformation.entrySet()) {
            final Tuple groupingKey = entry.getKey();
            LOGGER.debug(KeyValueLogMessage.of("Validating group",
                    "group", groupingKey,
                    "expectedCount", entry.getValue().size()));

            final List<Tuple> records = entry.getValue().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index, groupingKey);
            partitionInfos.sort(Comparator.comparing(info -> Tuple.fromBytes(info.getFrom().toByteArray())));
            final String allCounts = partitionInfos.stream()
                    .map(info -> Tuple.fromBytes(info.getFrom().toByteArray()).toString() + info.getCount())
                    .collect(Collectors.joining(",", "[", "]"));
            Set<Integer> usedPartitionIds = new HashSet<>();
            Tuple lastToTuple = null;
            int visitedCount = 0;

            try (FDBRecordContext context = contextProvider.get()) {
                final FDBRecordStore recordStore = schemaSetup.apply(context);

                for (int i = 0; i < partitionInfos.size(); i++) {
                    final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = partitionInfos.get(i);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Group: " + groupingKey + " PartitionInfo[" + partitionInfo.getId() +
                                "]: count:" + partitionInfo.getCount() + " " +
                                Tuple.fromBytes(partitionInfo.getFrom().toByteArray()) + "-> " +
                                Tuple.fromBytes(partitionInfo.getTo().toByteArray()));
                    }

                    assertTrue(isParititionCountWithinBounds(partitionInfos, i, partitionLowWatermark, partitionHighWatermark),
                            "Group: " + groupingKey + " - " + allCounts + "\nlowWatermark: " + partitionLowWatermark + ", highWatermark: " + partitionHighWatermark +
                                    "\nCurrent count: " + partitionInfo.getCount());
                    assertTrue(usedPartitionIds.add(partitionInfo.getId()), () -> "Duplicate id: " + partitionInfo);
                    final Tuple fromTuple = Tuple.fromBytes(partitionInfo.getFrom().toByteArray());
                    if (i > 0) {
                        assertThat(fromTuple, greaterThan(lastToTuple));
                    }
                    lastToTuple = Tuple.fromBytes(partitionInfo.getTo().toByteArray());
                    assertThat(fromTuple, lessThanOrEqualTo(lastToTuple));

                    LOGGER.debug(KeyValueLogMessage.of("Visited partition",
                            "group", groupingKey,
                            "documentsSoFar", visitedCount,
                            "documentsInGroup", records.size(),
                            "partitionInfo.count", partitionInfo.getCount()));
                    final Set<Tuple> expectedPrimaryKeys = Set.copyOf(records.subList(visitedCount, visitedCount + partitionInfo.getCount()));
                    validateDocsInPartition(recordStore, index, partitionInfo.getId(), groupingKey,
                            expectedPrimaryKeys,
                            universalSearch);
                    visitedCount += partitionInfo.getCount();
                    validatePrimaryKeySegmentIndex(recordStore, index, groupingKey, partitionInfo.getId(),
                            expectedPrimaryKeys, allowDuplicatePrimaryKeys);
                    expectedPrimaryKeys.forEach(primaryKey -> missingDocuments.get(groupingKey).remove(primaryKey));
                }
            }
        }
        missingDocuments.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        assertEquals(Map.of(), missingDocuments, "We should have found all documents in the index");
    }

    List<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionMeta(Index index,
                                                                                Tuple groupingKey) {
        try (FDBRecordContext context = contextProvider.get()) {
            final FDBRecordStore recordStore = schemaSetup.apply(context);
            LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer) recordStore.getIndexMaintainer(index);
            return indexMaintainer.getPartitioner().getAllPartitionMetaInfo(groupingKey).join();
        }
    }

    boolean isParititionCountWithinBounds(@Nonnull final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos,
                                          int currentPartitionIndex,
                                          int lowWatermark,
                                          int highWatermark) {
        int currentCount = partitionInfos.get(currentPartitionIndex).getCount();
        if (currentCount > highWatermark) {
            return false;
        }
        if (currentCount >= lowWatermark) {
            return true;
        }
        // here: count < lowWatermark
        int leftNeighborCapacity = currentPartitionIndex == 0 ? 0 : getPartitionExtraCapacity(partitionInfos.get(currentPartitionIndex - 1).getCount(), highWatermark);
        int rightNeighborCapacity = currentPartitionIndex == (partitionInfos.size() - 1) ? 0 : getPartitionExtraCapacity(partitionInfos.get(currentPartitionIndex + 1).getCount(), highWatermark);

        return currentCount > 0 && (leftNeighborCapacity + rightNeighborCapacity) < currentCount;
    }

    int getPartitionExtraCapacity(int count, int highWatermark) {
        return Math.max(0, highWatermark - count);
    }

    public static void validateDocsInPartition(final FDBRecordStore recordStore, Index index, int partitionId, Tuple groupingKey,
                                               Set<Tuple> expectedPrimaryKeys, final String universalSearch) throws IOException {
        LuceneScanQuery scanQuery;
        if (groupingKey.isEmpty()) {
            scanQuery = (LuceneScanQuery) LuceneIndexTestUtils.fullSortTextSearch(recordStore, index, universalSearch, null);
        } else {
            scanQuery = (LuceneScanQuery) groupedSortedTextSearch(recordStore, index,
                    universalSearch,
                    null,
                    groupingKey.getLong(0));
        }
        final IndexReader indexReader = getIndexReader(recordStore, index, groupingKey, partitionId);
        LuceneOptimizedIndexSearcher searcher = new LuceneOptimizedIndexSearcher(indexReader);
        TopDocs newTopDocs = searcher.search(scanQuery.getQuery(), Integer.MAX_VALUE);

        assertNotNull(newTopDocs);
        assertNotNull(newTopDocs.scoreDocs);
        assertEquals(expectedPrimaryKeys.size(), newTopDocs.scoreDocs.length);

        Set<String> fields = Set.of(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME);
        Assertions.assertEquals(expectedPrimaryKeys.stream().sorted().collect(Collectors.toList()), Arrays.stream(newTopDocs.scoreDocs)
                        .map(scoreDoc -> {
                            try {
                                Document document = searcher.doc(scoreDoc.doc, fields);
                                IndexableField primaryKey = document.getField(LuceneIndexMaintainer.PRIMARY_KEY_FIELD_NAME);
                                return Tuple.fromBytes(primaryKey.binaryValue().bytes);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .sorted()
                        .collect(Collectors.toList()),
                () -> index.getRootExpression() + " " + groupingKey + ":" + partitionId);
    }

    public static IndexReader getIndexReader(final FDBRecordStore recordStore, final Index index,
                                             final Tuple groupingKey, final int partitionId) throws IOException {
        final FDBDirectoryManager manager = getDirectoryManager(recordStore, index);
        return manager.getIndexReader(groupingKey, partitionId);
    }

    @Nonnull
    private static FDBDirectoryManager getDirectoryManager(final FDBRecordStore recordStore, final Index index) {
        IndexMaintainerState state = new IndexMaintainerState(recordStore, index, recordStore.getIndexMaintenanceFilter());
        return FDBDirectoryManager.getManager(state);
    }

    public static LuceneScanBounds groupedSortedTextSearch(final FDBRecordStoreBase<?> recordStore, Index index, String search, Sort sort, Object group) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))),
                new LuceneQuerySearchClause(LuceneQueryType.QUERY, search, false),
                sort,
                null,
                null,
                null);

        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    public static void validatePrimaryKeySegmentIndex(@Nonnull FDBRecordStore recordStore,
                                                      @Nonnull Index index,
                                                      @Nonnull Tuple groupingKey,
                                                      @Nullable Integer partitionId,
                                                      @Nonnull Set<Tuple> expectedPrimaryKeys,
                                                      final boolean allowDuplicates) throws IOException {
        final FDBDirectoryManager directoryManager = getDirectoryManager(recordStore, index);
        final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = directoryManager.getDirectory(groupingKey, partitionId)
                .getPrimaryKeySegmentIndex();
        final String message = "Group: " + groupingKey + ", partition: " + partitionId;
        if (Boolean.parseBoolean(index.getOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED)) ||
                Boolean.parseBoolean(index.getOption(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED)) ) {
            assertNotNull(primaryKeySegmentIndex, message);
            final List<List<Object>> allEntries = primaryKeySegmentIndex.readAllEntries();
            // sorting the two lists for easier reading on failures
            if (allowDuplicates) {
                assertEquals(new HashSet<>(expectedPrimaryKeys),
                        allEntries.stream()
                                .map(entry -> Tuple.fromList(entry.subList(0, entry.size() - 2)))
                                .collect(Collectors.toSet()),
                        message);
            } else {
                assertEquals(expectedPrimaryKeys.stream().sorted().collect(Collectors.toList()),
                        allEntries.stream()
                                .map(entry -> Tuple.fromList(entry.subList(0, entry.size() - 2)))
                                .sorted()
                                .collect(Collectors.toList()),
                        message);
            }
            directoryManager.getIndexWriter(groupingKey, partitionId, LuceneAnalyzerWrapper.getStandardAnalyzerWrapper());
            final DirectoryReader directoryReader = directoryManager.getDirectoryReader(groupingKey, partitionId);
            for (final Tuple primaryKey : expectedPrimaryKeys) {
                assertNotNull(primaryKeySegmentIndex.findDocument(directoryReader, primaryKey),
                        message + " " + primaryKey + " " + primaryKeySegmentIndex.findSegments(primaryKey));
            }
        } else {
            assertNull(primaryKeySegmentIndex, message);
        }
    }
}

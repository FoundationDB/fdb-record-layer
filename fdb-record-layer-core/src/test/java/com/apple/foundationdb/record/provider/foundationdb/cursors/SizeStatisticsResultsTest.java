/*
 * SizeStatisticsResultsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursorProto;
import com.google.common.base.Strings;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link SizeStatisticsResults}.
 */
public class SizeStatisticsResultsTest {

    @Test
    public void emptyStatistics() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        Assertions.assertThat(stats.getKeyCount()).isZero();
        Assertions.assertThat(stats.getKeySize()).isZero();
        Assertions.assertThat(stats.getMaxKeySize()).isZero();
        Assertions.assertThat(stats.getValueSize()).isZero();
        Assertions.assertThat(stats.getMaxValueSize()).isZero();
        Assertions.assertThat(stats.getTotalSize()).isZero();
        Assertions.assertThat(stats.getSizeBuckets()).hasSize(Integer.SIZE);
        Assertions.assertThat(stats.getSizeBuckets()).containsOnly(0L);
    }

    @Test
    public void updateStatisticsSingleKeyValue() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        KeyValue kv = new KeyValue("key".getBytes(), "value".getBytes());
        
        stats.updateStatistics(kv);
        
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(1);
        Assertions.assertThat(stats.getKeySize()).isEqualTo(3); // "key"
        Assertions.assertThat(stats.getMaxKeySize()).isEqualTo(3);
        Assertions.assertThat(stats.getValueSize()).isEqualTo(5); // "value"
        Assertions.assertThat(stats.getMaxValueSize()).isEqualTo(5);
        Assertions.assertThat(stats.getTotalSize()).isEqualTo(8); // 3 + 5
        Assertions.assertThat(stats.getAverageKeySize()).isEqualTo(3.0);
        Assertions.assertThat(stats.getAverageValueSize()).isEqualTo(5.0);
        Assertions.assertThat(stats.getAverage()).isEqualTo(8.0);
    }

    @Test
    public void updateStatisticsMultipleKeyValues() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        stats.updateStatistics(new KeyValue("a".getBytes(), "x".getBytes())); // 1 + 1 = 2
        stats.updateStatistics(new KeyValue("bb".getBytes(), "yy".getBytes())); // 2 + 2 = 4
        stats.updateStatistics(new KeyValue("ccc".getBytes(), "zzz".getBytes())); // 3 + 3 = 6
        
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(3);
        Assertions.assertThat(stats.getKeySize()).isEqualTo(6); // 1 + 2 + 3
        Assertions.assertThat(stats.getMaxKeySize()).isEqualTo(3);
        Assertions.assertThat(stats.getValueSize()).isEqualTo(6); // 1 + 2 + 3
        Assertions.assertThat(stats.getMaxValueSize()).isEqualTo(3);
        Assertions.assertThat(stats.getTotalSize()).isEqualTo(12); // 6 + 6
        Assertions.assertThat(stats.getAverageKeySize()).isEqualTo(2.0); // 6 / 3
        Assertions.assertThat(stats.getAverageValueSize()).isEqualTo(2.0); // 6 / 3
        Assertions.assertThat(stats.getAverage()).isEqualTo(4.0); // 12 / 3
    }

    @Test
    public void sizeBucketsCorrectDistribution() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Size 1: bucket 0 (since 2^0 = 1)
        stats.updateStatistics(new KeyValue("0".getBytes(), "".getBytes())); // total size 1
        
        // Size 2: bucket 1 (since 2^1 = 2)
        stats.updateStatistics(new KeyValue("a".getBytes(), "b".getBytes())); // total size 2
        
        // Size 4: bucket 2 (since 2^2 = 4)
        stats.updateStatistics(new KeyValue("ab".getBytes(), "cd".getBytes())); // total size 4
        
        // Size 8: bucket 3 (since 2^3 = 8)
        stats.updateStatistics(new KeyValue("abcd".getBytes(), "efgh".getBytes())); // total size 8
        
        long[] buckets = stats.getSizeBuckets();
        Assertions.assertThat(buckets[0]).isEqualTo(1); // size 1
        Assertions.assertThat(buckets[1]).isEqualTo(1); // size 2
        Assertions.assertThat(buckets[2]).isEqualTo(1); // size 4
        Assertions.assertThat(buckets[3]).isEqualTo(1); // size 8
        
        // All other buckets should be zero
        for (int i = 4; i < Integer.SIZE; i++) {
            Assertions.assertThat(buckets[i]).isZero();
        }
    }

    @Test
    public void sizeBucketsZeroSizeHandling() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Zero-size key-value pair should not be counted in buckets
        stats.updateStatistics(new KeyValue("".getBytes(), "".getBytes()));
        
        long[] buckets = stats.getSizeBuckets();
        Assertions.assertThat(buckets).containsOnly(0L);
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(1);
    }

    @Test
    public void proportionCalculation() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Add entries with known sizes to specific buckets
        for (int i = 0; i < 10; i++) {
            stats.updateStatistics(new KeyValue("a".getBytes(), Strings.repeat("a", i).getBytes()));
        }

        // Total: 30 entries, 10 in bucket 1, 20 in bucket 2
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(10);
        
        // Test various proportions
        double p10 = stats.getProportion(0.1); // 10% = 3 entries, should be in bucket 1
        double p50 = stats.getProportion(0.5); // 50% = 15 entries, should be in bucket 2
        double p90 = stats.getProportion(0.9); // 90% = 27 entries, should be in bucket 2
        
        Assertions.assertThat(p10).isGreaterThan(1.0).isLessThanOrEqualTo(2.0);
        Assertions.assertThat(p50).isGreaterThan(5.0).isLessThanOrEqualTo(6.0);
        Assertions.assertThat(p90).isGreaterThan(9.0); // bucket 2 range
    }

    @Test
    public void medianP90P95() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Add uniform distribution
        for (int i = 0; i < 100; i++) {
            stats.updateStatistics(new KeyValue("ab".getBytes(), "cd".getBytes())); // size 4
        }
        
        double median = stats.getMedian();
        double p90 = stats.getP90();
        double p95 = stats.getP95();
        
        Assertions.assertThat(median).isEqualTo(stats.getProportion(0.5));
        Assertions.assertThat(p90).isEqualTo(stats.getProportion(0.9));
        Assertions.assertThat(p95).isEqualTo(stats.getProportion(0.95));
    }

    @Test
    public void proportionEdgeCases() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        stats.updateStatistics(new KeyValue("a".getBytes(), "b".getBytes()));
        
        // Valid edge cases
        Assertions.assertThat(stats.getProportion(0.0)).isGreaterThan(0);
        Assertions.assertThat(stats.getProportion(0.99)).isGreaterThan(0);
        
        // Invalid proportions
        Assertions.assertThatThrownBy(() -> stats.getProportion(-0.1))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("proportion");
        
        Assertions.assertThatThrownBy(() -> stats.getProportion(1.0))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("proportion");
        
        Assertions.assertThatThrownBy(() -> stats.getProportion(1.5))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("proportion");
    }

    @Test
    public void copyMethod() {
        SizeStatisticsResults original = new SizeStatisticsResults();
        original.updateStatistics(new KeyValue("key".getBytes(), "value".getBytes()));
        original.updateStatistics(new KeyValue("key2".getBytes(), "value2".getBytes()));
        
        SizeStatisticsResults copy = original.copy();
        
        Assertions.assertThat(copy).usingRecursiveComparison().isEqualTo(original);

        // Verify it's a deep copy - modifying copy shouldn't affect original
        copy.updateStatistics(new KeyValue("new".getBytes(), "kv".getBytes()));
        Assertions.assertThat(copy).usingRecursiveComparison().isNotEqualTo(original);
    }

    @Test
    public void protoRoundTrip() {
        SizeStatisticsResults original = new SizeStatisticsResults();
        original.updateStatistics(new KeyValue("key1".getBytes(), "value1".getBytes()));
        original.updateStatistics(new KeyValue("key22".getBytes(), "value22".getBytes()));
        original.updateStatistics(new KeyValue("key333".getBytes(), "value333".getBytes()));
        
        // Convert to proto and back
        RecordCursorProto.SizeStatisticsPartialResults proto = original.toProto();
        SizeStatisticsResults restored = SizeStatisticsResults.fromProto(proto);
        
        // Verify all fields are preserved
        Assertions.assertThat(restored).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    public void protoWithEmptyStats() {
        SizeStatisticsResults original = new SizeStatisticsResults();
        
        RecordCursorProto.SizeStatisticsPartialResults proto = original.toProto();
        SizeStatisticsResults restored = SizeStatisticsResults.fromProto(proto);

        Assertions.assertThat(restored).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    public void largeKeyValueHandling() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Create large key and value
        final int keySize = 10000;
        final int valueSize = 20000;
        byte[] largeKey = new byte[keySize];
        Arrays.fill(largeKey, (byte) 'K');
        byte[] largeValue = new byte[valueSize];
        Arrays.fill(largeValue, (byte) 'V');
        
        stats.updateStatistics(new KeyValue(largeKey, largeValue));
        
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(1);
        Assertions.assertThat(stats.getKeySize()).isEqualTo(keySize);
        Assertions.assertThat(stats.getMaxKeySize()).isEqualTo(keySize);
        Assertions.assertThat(stats.getValueSize()).isEqualTo(valueSize);
        Assertions.assertThat(stats.getMaxValueSize()).isEqualTo(valueSize);
        final int totalSize = keySize + valueSize;
        Assertions.assertThat(stats.getTotalSize()).isEqualTo(totalSize);
        
        // Verify it goes into the correct bucket
        long[] buckets = stats.getSizeBuckets();
        int expectedBucket = Integer.SIZE - Integer.numberOfLeadingZeros(totalSize) - 1;
        Assertions.assertThat(buckets[expectedBucket]).isEqualTo(1);
    }

    @Test
    public void averageCalculationsWithZeroCount() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // With zero count, averages should be NaN due to division by zero
        Assertions.assertThat(stats.getAverageKeySize()).isNaN();
        Assertions.assertThat(stats.getAverageValueSize()).isNaN();
        Assertions.assertThat(stats.getAverage()).isNaN();
    }

    @Test
    public void keyWithZeroSizeValue() {
        SizeStatisticsResults stats = new SizeStatisticsResults();
        
        // Test key with zero-size value
        stats.updateStatistics(new KeyValue("key".getBytes(), "".getBytes()));
        
        Assertions.assertThat(stats.getKeyCount()).isEqualTo(1);
        Assertions.assertThat(stats.getKeySize()).isEqualTo(3); // "key"
        Assertions.assertThat(stats.getMaxKeySize()).isEqualTo(3);
        Assertions.assertThat(stats.getValueSize()).isZero(); // empty value
        Assertions.assertThat(stats.getMaxValueSize()).isZero();
        Assertions.assertThat(stats.getTotalSize()).isEqualTo(3); // key size only
        Assertions.assertThat(stats.getAverageKeySize()).isEqualTo(3.0);
        Assertions.assertThat(stats.getAverageValueSize()).isZero();
        Assertions.assertThat(stats.getAverage()).isEqualTo(3.0);
        
        // Check that it goes into the correct bucket (size 3)
        long[] buckets = stats.getSizeBuckets();
        int expectedBucket = Integer.SIZE - Integer.numberOfLeadingZeros(3) - 1;
        Assertions.assertThat(buckets[expectedBucket]).isEqualTo(1);
        
        // All other buckets should be zero
        for (int i = 0; i < Integer.SIZE; i++) {
            if (i != expectedBucket) {
                Assertions.assertThat(buckets[i]).isZero();
            }
        }
    }
}

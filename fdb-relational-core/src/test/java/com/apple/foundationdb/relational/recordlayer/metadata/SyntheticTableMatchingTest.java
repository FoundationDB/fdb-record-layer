/*
 * SyntheticTableMatchingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.google.common.collect.ImmutableMultiset;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for how synthetic tables producing the same synthetic records are recognized, whatever the aliases of their
 * constituents and the order the constituents were registered in: {@link RecordLayerSyntheticTable#getFingerprint()},
 * the matcher of each kind, which does not need the table a definition would add, the lookup of existing tables by
 * fingerprint, and the index registration of {@link RecordLayerSchemaTemplate.Builder}.
 */
class SyntheticTableMatchingTest {

    @Test
    void matcherOfUnnestedTableWithPermutedConstituentsMapsByArray() {
        final var table = unnested("__unnested_1", "T", "parent",
                nested("unnesting_0", "parent", fanOut("A")),
                nested("unnesting_1", "parent", fanOut("B")));
        final var permuted = unnested("__unnested_2", "T", "parent",
                nested("unnesting_0", "parent", fanOut("B")),
                nested("unnesting_1", "parent", fanOut("A")));

        assertThat(match(table, permuted)).contains(Map.of(
                "parent", "parent",
                "unnesting_0", "unnesting_1",
                "unnesting_1", "unnesting_0"));
    }

    @Test
    void matcherOfUnnestedTableWithChainedConstituentsMapsParentsConsistently() {
        // A, and C unnested from the elements of A, and B
        final var table = unnested("__unnested_1", "T", "parent",
                nested("unnesting_0", "parent", fanOut("A")),
                nested("unnesting_1", "unnesting_0", fanOut("C")),
                nested("unnesting_2", "parent", fanOut("B")));
        // the same, found in another order and named differently
        final var renamed = unnested("__unnested_2", "T", "row",
                nested("x", "row", fanOut("B")),
                nested("y", "row", fanOut("A")),
                nested("z", "y", fanOut("C")));

        assertThat(match(table, renamed)).contains(Map.of(
                "parent", "row",
                "unnesting_0", "y",
                "unnesting_1", "z",
                "unnesting_2", "x"));
        assertThat(match(renamed, table)).contains(Map.of(
                "row", "parent",
                "y", "unnesting_0",
                "z", "unnesting_1",
                "x", "unnesting_2"));
    }

    @Test
    void matcherOfUnnestedTableWithInterchangeableConstituentsMapsEachOnce() {
        final var table = unnested("__unnested_1", "T", "parent",
                nested("unnesting_0", "parent", fanOut("A")),
                nested("unnesting_1", "parent", fanOut("A")));
        final var other = unnested("__unnested_2", "T", "parent",
                nested("first", "parent", fanOut("A")),
                nested("second", "parent", fanOut("A")));

        final var mapping = match(table, other);
        assertThat(mapping).isPresent();
        assertThat(mapping.get()).containsEntry("parent", "parent");
        assertThat(mapping.get().values()).containsExactlyInAnyOrder("parent", "first", "second");
    }

    @Test
    void getFingerprintOfUnnestedTableOfOtherStructureDiffers() {
        // the fingerprint of an unnested table is exact, so tables of other structure never get as far as the matcher
        final var table = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")));
        // another array
        assertDifferentFingerprints(table, unnested("__unnested_2", "T", "parent", nested("unnesting_0", "parent", fanOut("B"))));
        // the same array, stored wrapped as a nullable array is
        assertDifferentFingerprints(table, unnested("__unnested_2", "T", "parent",
                nested("unnesting_0", "parent", field("A").nest(fanOut("values")))));
        // another stored table
        assertDifferentFingerprints(table, unnested("__unnested_2", "U", "parent", nested("unnesting_0", "parent", fanOut("A"))));
        // one more level
        assertDifferentFingerprints(table, unnested("__unnested_2", "T", "parent",
                nested("unnesting_0", "parent", fanOut("A")),
                nested("unnesting_1", "unnesting_0", fanOut("C"))));
        // the same arrays, but C unnested from the stored record rather than from the elements of A
        assertDifferentFingerprints(unnested("__unnested_1", "T", "parent",
                        nested("unnesting_0", "parent", fanOut("A")),
                        nested("unnesting_1", "unnesting_0", fanOut("C"))),
                unnested("__unnested_2", "T", "parent",
                        nested("unnesting_0", "parent", fanOut("A")),
                        nested("unnesting_1", "parent", fanOut("C"))));
    }

    @Test
    void getFingerprintOfUnnestedTableIsStable() {
        final var table = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")));
        assertThat(table.getFingerprint()).isEqualTo(new RecordLayerUnnestedSyntheticTable.UnnestedFingerprint("T",
                ImmutableMultiset.of(new RecordLayerUnnestedSyntheticTable.UnnestedFingerprint.Nesting(fanOut("A"),
                        ImmutableMultiset.of()))));
        // pinned: the hash code must not depend on anything but the structure, run or JVM included
        assertThat(table.getFingerprint().hashCode()).isEqualTo(-615066666);
    }

    @Test
    void matcherOfJoinedTableWithPermutedConstituentsAndSwappedConditionMapsByTable() {
        final var table = joined("__joined_1",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "B")),
                List.of(condition("joined_0", "K", "joined_1", "K2")));
        // B joined to A rather than A to B, the equality written the other way round
        final var permuted = joined("__joined_2",
                List.of(constituent("joined_0", "B"), constituent("joined_1", "A")),
                List.of(condition("joined_0", "K2", "joined_1", "K")));

        assertThat(match(table, permuted)).contains(Map.of("joined_0", "joined_1", "joined_1", "joined_0"));
    }

    @Test
    void matcherOfJoinedSelfJoinMapsCopiesByTheirConditions() {
        // two copies of T, told apart by the column each one joins U on
        final var table = joined("__joined_1",
                List.of(constituent("a", "T"), constituent("b", "T"), constituent("c", "U")),
                List.of(condition("a", "X", "c", "Y"), condition("b", "Z", "c", "Y")));
        final var renamed = joined("__joined_2",
                List.of(constituent("p", "U"), constituent("q", "T"), constituent("r", "T")),
                List.of(condition("q", "Z", "p", "Y"), condition("p", "Y", "r", "X")));

        assertThat(match(table, renamed)).contains(Map.of("a", "r", "b", "q", "c", "p"));
    }

    @Test
    void matcherOfJoinedTableOfOtherConditionsFindsNoMatch() {
        final var table = joined("__joined_1",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "B")),
                List.of(condition("joined_0", "K", "joined_1", "K")));
        // other columns: the same tables and as many conditions, so the same fingerprint
        assertNoMatchOfSameFingerprint(table, joined("__joined_2",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "B")),
                List.of(condition("joined_0", "K", "joined_1", "L"))));
        // one more condition
        assertDifferentFingerprints(table, joined("__joined_2",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "B")),
                List.of(condition("joined_0", "K", "joined_1", "K"), condition("joined_0", "L", "joined_1", "L"))));
        // another table
        assertDifferentFingerprints(table, joined("__joined_2",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "C")),
                List.of(condition("joined_0", "K", "joined_1", "K"))));
    }

    @Test
    void matcherOfJoinedTablesOfEqualFingerprintButOtherStructureFindsNoMatch() {
        // Two triangles and a hexagon of the same table joined on the same column: as many constituents and join
        // conditions over the same tables, and every constituent has two neighbours alike, but they are not the same join.
        final var constituents = List.of(constituent("a", "T"), constituent("b", "T"), constituent("c", "T"),
                constituent("d", "T"), constituent("e", "T"), constituent("f", "T"));
        final var triangles = joined("__joined_1", constituents, List.of(
                condition("a", "K", "b", "K"), condition("b", "K", "c", "K"), condition("c", "K", "a", "K"),
                condition("d", "K", "e", "K"), condition("e", "K", "f", "K"), condition("f", "K", "d", "K")));
        final var hexagon = joined("__joined_2", constituents, List.of(
                condition("a", "K", "b", "K"), condition("b", "K", "c", "K"), condition("c", "K", "d", "K"),
                condition("d", "K", "e", "K"), condition("e", "K", "f", "K"), condition("f", "K", "a", "K")));

        assertNoMatchOfSameFingerprint(triangles, hexagon);
        assertThat(match(triangles, triangles)).isPresent();
    }

    @Test
    void getFingerprintOfJoinedTableIsStable() {
        final var table = joined("__joined_1",
                List.of(constituent("joined_0", "U"), constituent("joined_1", "T"), constituent("joined_2", "T")),
                List.of(condition("joined_0", "K", "joined_1", "K"), condition("joined_1", "X", "joined_2", "Y")));
        // the number of join conditions and the tables, each as many times as it is joined, nothing else
        assertThat(table.getFingerprint()).isEqualTo(
                new RecordLayerJoinedSyntheticTable.JoinedFingerprint(2, ImmutableMultiset.of("T", "T", "U")));
        // pinned: the hash code must not depend on anything but the structure, run or JVM included
        assertThat(table.getFingerprint().hashCode()).isEqualTo(-2000691801);
    }

    @Test
    void getFingerprintOfJoinedSelfJoinCountsEachCopy() {
        // the same tables, as many constituents and conditions, but T is joined to itself rather than U
        final var table = joined("__joined_1",
                List.of(constituent("a", "T"), constituent("b", "T"), constituent("c", "U")),
                List.of(condition("a", "K", "b", "K"), condition("b", "K", "c", "K")));
        final var other = joined("__joined_2",
                List.of(constituent("a", "T"), constituent("b", "U"), constituent("c", "U")),
                List.of(condition("a", "K", "b", "K"), condition("b", "K", "c", "K")));

        assertDifferentFingerprints(table, other);
    }

    @Test
    void getFingerprintOfTablesOfOtherKindsDiffers() {
        final var unnested = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")));
        final var joined = joined("__joined_1",
                List.of(constituent("joined_0", "T"), constituent("joined_1", "A")),
                List.of(condition("joined_0", "K", "joined_1", "K")));
        assertDifferentFingerprints(unnested, joined);
    }

    @Test
    void withIndexKeepsFingerprintAndIndexOrder() {
        final var table = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")));
        final var withIndexes = table.withIndex(index(table, "I2")).withIndex(index(table, "I1")).withIndex(index(table, "I3"));

        assertThat(withIndexes.getFingerprint()).isEqualTo(table.getFingerprint());
        assertThat(withIndexes.getIndexes()).extracting(RecordLayerIndex::getName).containsExactly("I2", "I1", "I3");
    }

    @Test
    void withIndexOfExistingNameFails() {
        final var table = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")))
                .withIndex(index("__unnested_1", "I1"));
        assertThatThrownBy(() -> table.withIndex(index("__unnested_1", "I1")))
                .isInstanceOfSatisfying(UncheckedRelationalException.class, exception ->
                        assertThat(exception.unwrap().getErrorCode()).isEqualTo(ErrorCode.INDEX_ALREADY_EXISTS));
    }

    @Test
    void getSyntheticTablesReturnsTablesOfTheFingerprintInRegistrationOrder() {
        final var first = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")))
                .withIndex(index("__unnested_1", "I1"));
        final var second = unnested("__unnested_2", "T", "parent", nested("other", "parent", fanOut("A")))
                .withIndex(index("__unnested_2", "I2"));
        final var unrelated = unnested("__unnested_3", "T", "parent", nested("unnesting_0", "parent", fanOut("B")))
                .withIndex(index("__unnested_3", "I3"));
        final var builder = RecordLayerSchemaTemplate.newBuilder()
                .addSyntheticTable(unrelated)
                .addSyntheticTable(first)
                .addSyntheticTable(second);

        final var fingerprint = RecordLayerUnnestedSyntheticTable.generateFingerprint("T", "row",
                List.of(nested("x", "row", fanOut("A"))));
        assertThat(builder.getSyntheticTables(fingerprint)).containsExactly(first, second);

        final var later = unnested("__unnested_4", "T", "parent", nested("y", "parent", fanOut("A")))
                .withIndex(index("__unnested_4", "I4"));
        builder.addSyntheticTable(later);
        assertThat(builder.getSyntheticTables(fingerprint)).containsExactly(first, second, later);

        assertThat(builder.getSyntheticTables(RecordLayerUnnestedSyntheticTable.generateFingerprint("T", "parent",
                List.of(nested("unnesting_0", "parent", fanOut("D")))))).isEmpty();
        assertThat(RecordLayerSchemaTemplate.newBuilder().getSyntheticTables(fingerprint)).isEmpty();
    }

    @Test
    void matcherOfUnnestedConstituentsMapsOntoTheExistingConstituents() {
        final var existing = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")));
        final var matcher = RecordLayerUnnestedSyntheticTable.matcher("row", List.of(nested("x", "row", fanOut("A"))));
        assertThat(matcher.apply(existing)).contains(Map.of("row", "parent", "x", "unnesting_0"));
    }

    @Test
    void getSyntheticTablesOfJoinedFingerprintReturnsOnlyJoinedTables() {
        final var existing = joined("__joined_1",
                List.of(constituent("joined_0", "A"), constituent("joined_1", "B")),
                List.of(condition("joined_0", "K", "joined_1", "K2")))
                .withIndex(index("__joined_1", "I1"));
        final var unnestedOfSameTable = unnested("__unnested_1", "A", "parent", nested("unnesting_0", "parent", fanOut("B")))
                .withIndex(index("__unnested_1", "I2"));
        final var builder = RecordLayerSchemaTemplate.newBuilder()
                .addSyntheticTable(unnestedOfSameTable)
                .addSyntheticTable(existing);

        final var constituents = List.of(constituent("q", "B"), constituent("p", "A"));
        final var conditions = List.of(condition("q", "K2", "p", "K"));
        final var candidates = builder.getSyntheticTables(
                RecordLayerJoinedSyntheticTable.generateFingerprint(constituents, conditions));
        assertThat(candidates).containsExactly(existing);
        assertThat(RecordLayerJoinedSyntheticTable.matcher(constituents, conditions).apply(existing))
                .contains(Map.of("p", "joined_0", "q", "joined_1"));

        // same tables and as many conditions, so the same fingerprint, but other columns
        assertThat(RecordLayerJoinedSyntheticTable.matcher(
                List.of(constituent("p", "A"), constituent("q", "B")),
                List.of(condition("p", "K", "q", "K"))).apply(existing)).isEmpty();
    }

    @Test
    void addIndexDefinesIndexOnSyntheticTable() {
        final var table = unnested("__unnested_1", "T", "parent", nested("unnesting_0", "parent", fanOut("A")))
                .withIndex(index("__unnested_1", "I1"));
        final var builder = RecordLayerSchemaTemplate.newBuilder()
                .addSyntheticTable(table)
                .addIndex(index("__unnested_1", "I2"));

        assertThat(builder.getSyntheticTables(table.getFingerprint()))
                .singleElement()
                .satisfies(found -> assertThat(found.getIndexes()).extracting(RecordLayerIndex::getName)
                        .containsExactly("I1", "I2"));
    }

    @Test
    void addIndexOnUnknownTableFails() {
        final var builder = RecordLayerSchemaTemplate.newBuilder();
        assertThatThrownBy(() -> builder.addIndex(index("__unnested_1", "I1")))
                .isInstanceOfSatisfying(UncheckedRelationalException.class, exception ->
                        assertThat(exception.unwrap().getErrorCode()).isEqualTo(ErrorCode.UNDEFINED_TABLE));
    }

    /**
     * How {@code table} corresponds to {@code other}, the way a definition is matched onto an existing table: only
     * tables of the same fingerprint are compared, by a matcher built from the constituents alone.
     */
    @Nonnull
    private static Optional<Map<String, String>> match(@Nonnull final RecordLayerUnnestedSyntheticTable table,
                                                       @Nonnull final RecordLayerUnnestedSyntheticTable other) {
        assertThat(table.getFingerprint()).isEqualTo(other.getFingerprint());
        return RecordLayerUnnestedSyntheticTable.matcher(table.getAlias(), table.getConstituents()).apply(other);
    }

    /** As {@link #match(RecordLayerUnnestedSyntheticTable, RecordLayerUnnestedSyntheticTable)}, for joined tables. */
    @Nonnull
    private static Optional<Map<String, String>> match(@Nonnull final RecordLayerJoinedSyntheticTable table,
                                                       @Nonnull final RecordLayerJoinedSyntheticTable other) {
        assertThat(table.getFingerprint()).isEqualTo(other.getFingerprint());
        return RecordLayerJoinedSyntheticTable.matcher(table.getConstituents(), table.getJoinConditions()).apply(other);
    }

    /** Asserts the two tables are never compared at all, as they are looked up by different fingerprints. */
    private static void assertDifferentFingerprints(@Nonnull final RecordLayerSyntheticTable table,
                                                    @Nonnull final RecordLayerSyntheticTable other) {
        assertThat(table.getFingerprint()).isNotEqualTo(other.getFingerprint());
    }

    /** Asserts that two joined tables of the same fingerprint do not produce the same synthetic records. */
    private static void assertNoMatchOfSameFingerprint(@Nonnull final RecordLayerJoinedSyntheticTable table,
                                                       @Nonnull final RecordLayerJoinedSyntheticTable other) {
        assertThat(match(table, other)).isEmpty();
        assertThat(match(other, table)).isEmpty();
    }

    @Nonnull
    private static RecordLayerUnnestedSyntheticTable unnested(@Nonnull final String name,
                                                              @Nonnull final String parentTableName,
                                                              @Nonnull final String parentAlias,
                                                              @Nonnull final RecordLayerUnnestedSyntheticTable.NestedConstituent... constituents) {
        final var builder = RecordLayerUnnestedSyntheticTable.newBuilder(recordType(name))
                .setAlias(parentAlias)
                .setParentTableType(recordType(parentTableName));
        for (final var constituent : constituents) {
            builder.addConstituent(constituent);
        }
        return builder.build();
    }

    @Nonnull
    private static RecordLayerUnnestedSyntheticTable.NestedConstituent nested(@Nonnull final String alias,
                                                                              @Nonnull final String parentAlias,
                                                                              @Nonnull final KeyExpression nesting) {
        return new RecordLayerUnnestedSyntheticTable.NestedConstituent(alias, parentAlias, nesting);
    }

    @Nonnull
    private static KeyExpression fanOut(@Nonnull final String fieldName) {
        return field(fieldName, KeyExpression.FanType.FanOut);
    }

    @Nonnull
    private static RecordLayerJoinedSyntheticTable joined(@Nonnull final String name,
                                                          @Nonnull final List<RecordLayerJoinedSyntheticTable.JoinedConstituent> constituents,
                                                          @Nonnull final List<RecordLayerJoinedSyntheticTable.JoinCondition> conditions) {
        final var builder = RecordLayerJoinedSyntheticTable.newBuilder(recordType(name));
        constituents.forEach(builder::addConstituent);
        conditions.forEach(builder::addJoinCondition);
        return builder.build();
    }

    @Nonnull
    private static RecordLayerJoinedSyntheticTable.JoinedConstituent constituent(@Nonnull final String alias,
                                                                                 @Nonnull final String tableName) {
        return new RecordLayerJoinedSyntheticTable.JoinedConstituent(alias, tableName, tableName);
    }

    @Nonnull
    private static RecordLayerJoinedSyntheticTable.JoinCondition condition(@Nonnull final String leftAlias,
                                                                           @Nonnull final String leftField,
                                                                           @Nonnull final String rightAlias,
                                                                           @Nonnull final String rightField) {
        return new RecordLayerJoinedSyntheticTable.JoinCondition(leftAlias, field(leftField), rightAlias, field(rightField));
    }

    /**
     * A record type of the given name. Which fields it has does not matter here, as tables are matched by their
     * constituents rather than by their types.
     */
    @Nonnull
    private static Type.Record recordType(@Nonnull final String name) {
        return Type.Record.fromFieldsWithName(name, false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("ID"))));
    }

    @Nonnull
    private static RecordLayerIndex index(@Nonnull final RecordLayerSyntheticTable table, @Nonnull final String name) {
        return index(table.getName(), name);
    }

    @Nonnull
    private static RecordLayerIndex index(@Nonnull final String tableName, @Nonnull final String name) {
        return RecordLayerIndex.newBuilder()
                .setName(name)
                .setTableType(recordType(tableName))
                .setIndexType(IndexTypes.VALUE)
                .setKeyExpression(field("ID"))
                .build();
    }
}

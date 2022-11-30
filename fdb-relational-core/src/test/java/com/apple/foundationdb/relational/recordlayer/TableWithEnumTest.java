/*
 * TableWithEnumTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ContextualSQLException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class TableWithEnumTest {
    private static final List<String> SUITS = List.of("SPADES", "HEARTS", "DIAMONDS", "CLUBS");

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, TableWithEnumTest.class, TestSchemas.playingCard());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void canInsertAndGetSingleRecord() throws Exception {
        Message inserted = insertCard(42, "HEARTS", 8);
        assertThat(inserted.getField(inserted.getDescriptorForType().findFieldByName("SUIT")))
                .as("inserted value should set enum field")
                .isEqualTo(inserted.getDescriptorForType().findFieldByName("SUIT").getEnumType().findValueByName("HEARTS"));

        KeySet keys = new KeySet()
                .setKeyColumn("ID", 42);

        try (RelationalResultSet resultSet = statement.executeGet("CARD", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(inserted)
                    .hasNoNextRow();
        }
    }

    @Test
    void canInsertFiftyTwoCards() throws Exception {
        insert52Cards();

        long cardId = 0;
        for (String suit : SUITS) {
            for (int rank = 1; rank < 14; rank++) {
                KeySet keys = new KeySet()
                        .setKeyColumn("ID", ++cardId);
                final long longRank = rank;
                try (RelationalResultSet resultSet = statement.executeGet("CARD", keys, Options.NONE)) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .row()
                            .satisfies(val -> {
                                try {
                                    assertThat(val.getString("SUIT"))
                                            .isEqualTo(suit);
                                    assertThat(val.getLong("RANK"))
                                            .isEqualTo(longRank);
                                } catch (SQLException sqlErr) {
                                    fail("Encountered SQL error during execution", sqlErr);
                                }
                            });
                }
            }
        }
    }

    @Test
    void insertUnexpectedEnumValue() {
        RelationalAssertions.assertThrowsSqlException(() -> statement.getDataBuilder("CARD")
                .setField("ID", -1)
                .setField("SUIT", "TAILORED")
                .build())
                .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE)
                .hasMessageContaining("Invalid enum value");

        RelationalAssertions.assertThrowsSqlException(() -> statement.getDataBuilder("CARD")
                .setField("ID", -1)
                .setField("SUIT", 2)
                .build())
                .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE)
                .hasMessageContaining("Invalid enum value");
    }

    @Test
    @Disabled // TODO (bug: (fdb-record-layer) add support for enum in deepCopyIfNeeded() in RecordConstructorValue)
    void sortBySuit() throws Exception {
        insert52Cards();
        Assert.that(statement.execute("SELECT * FROM Card ORDER BY suit"), "Did not return a result set when one was expected");
        try (final RelationalResultSet resultSet = statement.getResultSet()) {
            var assertion = ResultSetAssert.assertThat(resultSet);
            int pk = 1;
            for (String suit : SUITS) {
                for (int rank = 1; rank < 14; rank++) {
                    assertion.hasNextRow();
                    assertion.hasRowExactly((long) pk++, "SPADES");
                }
            }
        }
    }

    @Test
    void filterBySuit() throws Exception {
        insert52Cards();

        // TODO: Enums need to be supported for comparison in the type repository for these queries to work
        assertThatThrownBy(() -> statement.execute("SELECT * FROM card WHERE card.suit = 'CLUBS'"))
                .isInstanceOf(ContextualSQLException.class)
                .hasMessageContaining("primitive type");

        assertThatThrownBy(() -> statement.execute("SELECT * FROM card WHERE card.suit > 'HEARTS'"))
                .isInstanceOf(ContextualSQLException.class)
                .hasMessageContaining("primitive type");
    }

    @Test
    void unsetSuit() throws RelationalException, SQLException {
        Message card = statement.getDataBuilder("CARD")
                .setField("ID", 0L)
                .build();
        assertThat(statement.executeInsert("CARD", card))
                .as("Should be able to insert card without suit")
                .isEqualTo(1);

        KeySet keys = new KeySet()
                .setKeyColumn("ID", 0L);
        try (RelationalResultSet resultSet = statement.executeGet("CARD", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(card)
                    .row()
                    .hasValue("SUIT", null);
        }
    }

    /**
     * Inserting a value via a query is not yet supported. Once it is, however, we should validate that proper
     * enum conversions are performed. Additionally, it would be good to make sure we test what happens if
     * an invalid enum value is specified.
     */
    @Test
    void insertViaQuery() {
        assertThatThrownBy(() -> statement.execute("INSERT INTO Card (id, suit, rank) VALUES (1, 'HEARTS', 4)"))
                .isInstanceOf(ContextualSQLException.class)
                .hasMessageContaining("query is not supported");
    }

    private void insert52Cards() throws Exception {
        connection.setAutoCommit(false);
        int cardId = 0;
        for (String suit : SUITS) {
            // For ranks, 1=ace, 2-10=numbers, 11=Jack/Knave, 12=Queen, and 13=King
            for (int rank = 1; rank < 14; rank++) {
                insertCard(++cardId, suit, rank);
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
        assertThat(cardId)
                .as("Should have inserted 52 cards")
                .isEqualTo(52);
    }

    private Message insertCard(long id, String suit, int rank) throws Exception {
        Message card = statement.getDataBuilder("CARD")
                .setField("ID", id)
                .setField("SUIT", suit)
                .setField("RANK", rank)
                .build();
        int insertCount = statement.executeInsert("CARD", card);
        assertThat(insertCount)
                .as("Did not count insertions correctly!")
                .isEqualTo(1);
        return card;
    }
}

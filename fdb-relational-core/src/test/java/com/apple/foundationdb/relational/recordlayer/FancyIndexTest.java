/*
 * FancyIndexTest.java
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

import com.apple.foundationdb.record.Leaderboard;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

/**
 * Tests that have to do with unusual indices, like leaderboards
 */
public class FancyIndexTest {
    @RegisterExtension
    public final KeySpaceRule keySpace = new KeySpaceRule("record_layer_index_test", "test",
            metaDataBuilder -> {
                metaDataBuilder.setRecords(Leaderboard.getDescriptor());
                GroupingKeyExpression scoreKeyExpr = Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                        .split(1)
                        .groupBy(Key.Expressions.field("game_id"));
                metaDataBuilder.addIndex("FlatLeaderboardRecord", new Index("score_idx", scoreKeyExpr, IndexTypes.COUNT_NOT_NULL));
            }
    );


    @Test
    void canQueryNestedIndex() {
        Leaderboard.FlatLeaderboardRecord r = Leaderboard.FlatLeaderboardRecord.newBuilder().setName("game-" + System.currentTimeMillis()).setGameId("chess").addScores(12).addScores(24).build();
        try (RecordStoreConnection recStoreConn = keySpace.openDirectConnection();
             Statement statement = recStoreConn.createStatement()) {
            statement.executeInsert("FlatLeaderboardRecord", Collections.singleton(r), Options.create());

            try(RelationalResultSet resultSet = statement.executeQuery("explain plan for select game_id, scores from FlatLeaderboardRecord/*+ INDEX(score_idx) */ where game_id = '" + r.getGameId() + "'", Options.create())) {
                Assertions.assertTrue(resultSet.next(), "Did not return rows");
                if (resultSet.supportsMessageParsing()) {
                    final Message row = resultSet.parseMessage();
                    System.out.println(row);
                } else {
                    String name = resultSet.getString(0);
                    System.out.println(name);
//            Assertions.assertEquals(name,r.getName(),"Did not match names");
                }
            }
        }
    }

}

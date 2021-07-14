/*
 * Demo.java
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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public class Demo {

    @RegisterExtension
    public final KeySpaceRule keySpace = new KeySpaceRule("record_layer_index_test", "test",
            metaDataBuilder -> {
                metaDataBuilder.setRecords(Restaurant.getDescriptor())
                        .getRecordType("RestaurantRecord")
                        .setPrimaryKey(field("rest_no"));

                metaDataBuilder.addIndex("RestaurantRecord", "addr_idx", Key.Expressions.concat(
                        field("rest_no"),
                        field("location").nest(field("address"))
                ));
            }
    );

    @Test
    public void demo() throws IOException {
        RecordStoreConnection recStoreConn = keySpace.openDirectConnection();

        printExplain(recStoreConn, "select * from RestaurantRecord");
        printExplain(recStoreConn, "select name from RestaurantRecord");
        printExplain(recStoreConn, "select * from RestaurantRecord where name = 'alpha'");
        printExplain(recStoreConn, "select name from RestaurantRecord where name = 'alpha'");
        printExplain(recStoreConn, "select name from RestaurantRecord order by name");
        printExplain(recStoreConn, "select rest_no from RestaurantRecord");
        printExplain(recStoreConn, "select rest_no from RestaurantRecord where name = 'alpha'");
        printExplain(recStoreConn, "select RestaurantRecord.location.address from RestaurantRecord");
        printExplain(recStoreConn, "select rest_no,RestaurantRecord.location.address from RestaurantRecord");
        printExplain(recStoreConn, "select rest_no,RestaurantRecord.location.address from RestaurantRecord where rest_no = 12");
    }

    private void printExplain(RecordStoreConnection conn, String query) throws IOException {
        System.out.printf("Query: \"%s\"%n", query);

        try(Statement statement = conn.createStatement()) {
            String explained = null;
            long duration = 0L;
            for (int i = 0; i < 5; i++) {
                long start = System.nanoTime();
                explained = statement.explainQuery(query, Options.create());
                duration = System.nanoTime() - start;
            }
            System.out.println(explained);
            System.out.printf("Query explained in %.3f millis%n", duration / 1_000_000f);
            System.out.println("-------------");
        }

    }
}

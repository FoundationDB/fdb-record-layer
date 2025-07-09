/*
 * Utils.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Environment;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * set of utility functions to generate PB objects for testing.
 */
public final class Utils {

    static Random r = new Random(42);

    static volatile AtomicLong sequencer = new AtomicLong();

    static List<RelationalStruct> generateRestaurantRecords(int count) {
        return IntStream.range(0, count).mapToObj(ignore -> Utils.generateRestaurantRecordWrapped()).collect(Collectors.toList());
    }

    static RelationalStruct generateRestaurantRecordWrapped() {
        int numReviews = r.nextInt(5) + 1;
        int numTags = r.nextInt(5) + 1;
        int numCustomers = r.nextInt(5) + 1;

        try {
            return EmbeddedRelationalStruct.newBuilder()
                    .addLong("REST_NO", sequencer.incrementAndGet())
                    .addString("NAME", "restName" + r.nextInt())
                    .addStruct("LOCATION", generateLocation())
                    .addArray("REVIEWS", generateList(numReviews, Utils::generateReview, Types.STRUCT))
                    .addArray("TAGS", generateList(numTags, Utils::generateTag, Types.STRUCT))
                    .addArray("CUSTOMER", generateList(numCustomers, () -> "cust" + r.nextInt(), Types.VARCHAR))
                    .build();
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    private static RelationalStruct generateLocation() {
        try {
            return EmbeddedRelationalStruct.newBuilder()
                    .addString("ADDRESS", "addr" + r.nextInt())
                    .addString("LATITUDE", "lat" + r.nextInt())
                    .addString("LONGITUDE", "long" + r.nextInt())
                    .build();
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    private static RelationalStruct generateReview() {
        try {
            return EmbeddedRelationalStruct.newBuilder()
                    .addLong("REVIEWER", r.nextInt())
                    .addLong("RATING", r.nextInt(5))
                    .build();
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    private static RelationalStruct generateTag() {
        try {
            return EmbeddedRelationalStruct.newBuilder()
                    .addString("TAG", "tag" + r.nextInt())
                    .addLong("WEIGHT", r.nextInt())
                    .build();
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    private static <T> RelationalArray generateList(int count, Supplier<T> supplier, int elementType) throws SQLException {
        assert count >= 0;

        final var builder = EmbeddedRelationalArray.newBuilder();
        for (int i = 0; i < count; i++) {
            switch (elementType) {
                case Types.STRUCT:
                    builder.addStruct((RelationalStruct) supplier.get());
                    break;
                case Types.VARCHAR:
                    builder.addString((String) supplier.get());
                    break;
                default:
                    throw new RelationalException("Not implemented!", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
            }
        }
        return builder.build();
    }

    /**
     * Enables internal Cascades debugger which, among other things, sets plan identifiers in a stable fashion making
     * it easier to view plans and reproduce planning steps.
     */
    public static void enableCascadesDebugger() {
        if (Debugger.getDebugger() == null && Environment.isDebug()) {
            Debugger.setDebugger(DebuggerWithSymbolTables.withoutSanityChecks());
        }
        Debugger.setup();
    }

    private Utils() {
    }
}

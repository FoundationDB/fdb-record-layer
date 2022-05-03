/*
 * PlanGenerationStackTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.net.URI;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A series of tests that target the following.
 * <ul>
 *     <li>Parsing</li>
 *     <li>Semantic Checks</li>
 *     <li>Logical plan generation</li>
 *     <li>Physical plan generation</li>
 * </ul>
 */
public class PlanGenerationStackTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            URI.create("/" + PlanGenerationStackTest.class.getSimpleName()), TestSchemas.restaurant());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    static class RandomQueryProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            return Stream.of(
                    Arguments.of(1, "select * from RestaurantRecord", null),
                    Arguments.of(2, "sElEct * FrOm RestaurantRecord", null),
                    Arguments.of(3, "   select *   from     RestaurantRecord", null),
                    Arguments.of(4, "sElEct * FrOm RestaUrantRecord", "Unknown record type RestaUrantRecord"),
                    Arguments.of(5, "incorrect ", "Syntax error at line 1 position 0"),
                    Arguments.of(6, "select * union RestaurantRecord", "Syntax error at line 1 position 15"),
                    Arguments.of(7, "select * from RestaurantRecord where rest_no > 10 ", null),
                    Arguments.of(8, "select * from RestaurantRecord where rest_no < 10 ", null),
                    Arguments.of(9, "select * from RestaurantRecord where rest_no = 10 ", null),
                    Arguments.of(10, "select * from RestaurantRecord where rest_no >= 10 ", null),
                    Arguments.of(11, "select * from RestaurantRecord where rest_no <= 10 ", null),
                    Arguments.of(12, "select * from RestaurantRecord where rest_no is null ", null),
                    Arguments.of(13, "select * from RestaurantRecord where rest_no is not null ", null),
                    Arguments.of(14, "select * from RestaurantRecord where NON_EXISTING > 10 ", "attempting to query non existing field NON_EXISTING"),
                    Arguments.of(15, "select * from RestaurantRecord where rest_no > 'hello'", "unable to encapsulate comparison operation due to type mismatch(es)"),
                    Arguments.of(16, "select * from RestaurantRecord where rest_no > 10 AND rest_no < 20", null),
                    Arguments.of(17, "select * from RestaurantRecord where rest_no < 10 AND rest_no < 20", null),
                    Arguments.of(18, "select * from RestaurantRecord where rest_no = 10 AND rest_no < 20", null),
                    Arguments.of(19, "select * from RestaurantRecord where rest_no >= 10 AND rest_no < 20", null),
                    Arguments.of(20, "select * from RestaurantRecord where rest_no <= 10 AND rest_no < 20", null),
                    Arguments.of(21, "select * from RestaurantRecord where rest_no is null AND rest_no < 20", null),
                    Arguments.of(22, "select * from RestaurantRecord where rest_no is not null AND rest_no < 20", null),
                    Arguments.of(23, "select * from RestaurantRecord where rest_no > 10 OR rest_no > 40", null),
                    Arguments.of(24, "select * from RestaurantRecord where rest_no < 10 OR rest_no > 40", null),
                    Arguments.of(25, "select * from RestaurantRecord where rest_no = 10 OR rest_no > 40", null),
                    Arguments.of(26, "select * from RestaurantRecord where rest_no >= 10 OR rest_no > 40", null),
                    Arguments.of(27, "select * from RestaurantRecord where rest_no <= 10 OR rest_no > 40", null),
                    Arguments.of(28, "select * from RestaurantRecord where rest_no is null OR rest_no > 40", null),
                    Arguments.of(29, "select * from RestaurantRecord where rest_no is not null OR rest_no > 40", null),
                    Arguments.of(30, "select * from RestaurantRecord where 42 > rest_no AND 42 < rest_no", null),
                    Arguments.of(31, "select * from RestaurantRecord where 42 < rest_no AND 42 < rest_no", null),
                    Arguments.of(32, "select * from RestaurantRecord where 42 = rest_no AND 42 < rest_no", null),
                    Arguments.of(33, "select * from RestaurantRecord where 42 >= rest_no AND 42 < rest_no", null),
                    Arguments.of(34, "select * from RestaurantRecord where 42 <= rest_no AND 42 < rest_no", null),
                    Arguments.of(35, "select * from RestaurantRecord where 42 is null AND 42 < rest_no", null),
                    Arguments.of(36, "select * from RestaurantRecord where 42 is not null AND 42 < rest_no", null),
                    Arguments.of(37, "select * from RestaurantRecord where 42 > rest_no OR 42 > rest_no", null),
                    Arguments.of(38, "select * from RestaurantRecord where 42 < rest_no OR 42 > rest_no", null),
                    Arguments.of(39, "select * from RestaurantRecord where 42 = rest_no OR 42 > rest_no", null),
                    Arguments.of(40, "select * from RestaurantRecord where 42 >= rest_no OR 42 > rest_no", null),
                    Arguments.of(41, "select * from RestaurantRecord where 42 <= rest_no OR 42 > rest_no", null),
                    Arguments.of(42, "select * from RestaurantRecord where 42 is null OR 42 > rest_no", null),
                    Arguments.of(43, "select * from RestaurantRecord where 42 is not null OR 42 > rest_no", null),
                    Arguments.of(44, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 > rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(45, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 < rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(46, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 = rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(47, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 >= rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(48, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 <= rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(49, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 is null AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(50, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 is not null AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(51, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 > rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(52, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 < rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(53, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 = rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(54, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 >= rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(55, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 <= rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(56, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 is null OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(57, "select * from RestaurantRecord where (((42 + 3) - 2) + 6 is not null OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(58, "select * from RestaurantRecord where rest_no is null", null),
                    Arguments.of(58, "select * from RestaurantRecord where rest_no is not null", null)
            );
        }
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @ArgumentsSource(RandomQueryProvider.class)
    void queryTestHarness(@Nonnull final int index, @Nonnull final String query, @Nullable String error) throws Exception {
        final String schemaName = connection.getSchema();
        final FDBRecordStore store = ((RecordStoreConnection) connection.connection).frl.loadSchema(schemaName, Options.create()).loadStore();

        if (error == null) {
            Assertions.assertDoesNotThrow(() -> PlanGenerator.generatePlan(query, store.getRecordMetaData(), store.getRecordStoreState()));
        } else {
            try {
                PlanGenerator.generatePlan(query, store.getRecordMetaData(), store.getRecordStoreState());
                Assertions.fail("expected an exception to be thrown");
            } catch (RelationalException e) {
                assertThat(e.getMessage()).contains(error);
            } catch (Exception e) {
                Assertions.fail("unexpected exception type " + e);
            }
        }
    }
}

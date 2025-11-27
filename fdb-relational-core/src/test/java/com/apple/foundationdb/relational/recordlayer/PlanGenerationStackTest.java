/*
 * PlanGenerationStackTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(PlanGenerationStackTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public PlanGenerationStackTest() {
        Utils.enableCascadesDebugger();
    }

    static class RandomQueryProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            return Stream.of(
                    Arguments.of(0, "select count(*) from restaurant", null),
                    Arguments.of(1, "select * from restaurant", null),
                    Arguments.of(1, "select * from restaurant where rest_no > -10", null),
                    Arguments.of(2, "sElEct * FrOm rESTaurant", null),
                    Arguments.of(3, "   select *   from     restaurant", null),
                    Arguments.of(4, "sElEct * FrOm \"RestaUrantRecord\"", "Unknown table RestaUrantRecord"),
                    Arguments.of(5, "incorrect ", "incorrect[[]]^^^^^^^^^"),
                    Arguments.of(6, "select * union restaurant", "select * union restaurant[[]]               ^^^^^^^^^^"),
                    Arguments.of(7, "select * from restaurant where rest_no > 10 ", null),
                    Arguments.of(8, "select * from restaurant where rest_no < 10 ", null),
                    Arguments.of(9, "select * from restaurant where rest_no = 10 ", null),
                    Arguments.of(10, "select * from restaurant where rest_no >= 10 ", null),
                    Arguments.of(11, "select * from restaurant where rest_no <= 10 ", null),
                    Arguments.of(12, "select * from restaurant where rest_no is null ", null),
                    Arguments.of(13, "select * from restaurant where rest_no is not null ", null),
                    Arguments.of(14, "select * from restaurant where NON_EXISTING > 10 ", "Attempting to query non existing column NON_EXISTING"),
                    Arguments.of(15, "select * from restaurant where rest_no > 'hello'", "unable to encapsulate comparison operation due to type mismatch(es)"),
                    Arguments.of(16, "select * from restaurant where rest_no > 10 AND rest_no < 20", null),
                    Arguments.of(17, "select * from restaurant where rest_no < 10 AND rest_no < 20", null),
                    Arguments.of(18, "select * from restaurant where rest_no = 10 AND rest_no < 20", null),
                    Arguments.of(19, "select * from restaurant where rest_no >= 10 AND rest_no < 20", null),
                    Arguments.of(20, "select * from restaurant where rest_no <= 10 AND rest_no < 20", null),
                    Arguments.of(21, "select * from restaurant where rest_no is null AND rest_no < 20", null),
                    Arguments.of(22, "select * from restaurant where rest_no is not null AND rest_no < 20", null),
                    Arguments.of(23, "select * from restaurant where rest_no > 10 OR rest_no > 40", null),
                    Arguments.of(24, "select * from restaurant where rest_no < 10 OR rest_no > 40", null),
                    Arguments.of(25, "select * from restaurant where rest_no = 10 OR rest_no > 40", null),
                    Arguments.of(26, "select * from restaurant where rest_no >= 10 OR rest_no > 40", null),
                    Arguments.of(27, "select * from restaurant where rest_no <= 10 OR rest_no > 40", null),
                    Arguments.of(28, "select * from restaurant where rest_no is null OR rest_no > 40", null),
                    Arguments.of(29, "select * from restaurant where rest_no is not null OR rest_no > 40", null),
                    Arguments.of(30, "select * from restaurant where 42 > rest_no AND 42 < rest_no", null),
                    Arguments.of(31, "select * from restaurant where 42 < rest_no AND 42 < rest_no", null),
                    Arguments.of(32, "select * from restaurant where 42 = rest_no AND 42 < rest_no", null),
                    Arguments.of(33, "select * from restaurant where 42 >= rest_no AND 42 < rest_no", null),
                    Arguments.of(34, "select * from restaurant where 42 <= rest_no AND 42 < rest_no", null),
                    Arguments.of(35, "select * from restaurant where 42 is null AND 42 < rest_no", null),
                    Arguments.of(36, "select * from restaurant where 42 is not null AND 42 < rest_no", null),
                    Arguments.of(37, "select * from restaurant where 42 > rest_no OR 42 > rest_no", null),
                    Arguments.of(38, "select * from restaurant where 42 < rest_no OR 42 > rest_no", null),
                    Arguments.of(39, "select * from restaurant where 42 = rest_no OR 42 > rest_no", null),
                    Arguments.of(40, "select * from restaurant where 42 >= rest_no OR 42 > rest_no", null),
                    Arguments.of(41, "select * from restaurant where 42 <= rest_no OR 42 > rest_no", null),
                    Arguments.of(42, "select * from restaurant where 42 is null OR 42 > rest_no", null),
                    Arguments.of(43, "select * from restaurant where 42 is not null OR 42 > rest_no", null),
                    Arguments.of(44, "select * from restaurant where (((42 + 3) - 2) + 6 > rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(45, "select * from restaurant where (((42 + 3) - 2) + 6 < rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(46, "select * from restaurant where (((42 + 3) - 2) + 6 = rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(47, "select * from restaurant where (((42 + 3) - 2) + 6 >= rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(48, "select * from restaurant where (((42 + 3) - 2) + 6 <= rest_no AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(49, "select * from restaurant where (((42 + 3) - 2) + 6 is null AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(50, "select * from restaurant where (((42 + 3) - 2) + 6 is not null AND ((42 + 3) - 2) + 6 < rest_no) OR (name = 'foo')", null),
                    Arguments.of(51, "select * from restaurant where (((42 + 3) - 2) + 6 > rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(52, "select * from restaurant where (((42 + 3) - 2) + 6 < rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(53, "select * from restaurant where (((42 + 3) - 2) + 6 = rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(54, "select * from restaurant where (((42 + 3) - 2) + 6 >= rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(55, "select * from restaurant where (((42 + 3) - 2) + 6 <= rest_no OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(56, "select * from restaurant where (((42 + 3) - 2) + 6 is null OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(57, "select * from restaurant where (((42 + 3) - 2) + 6 is not null OR ((42 + 3) - 2) + 6 > rest_no) OR (name = 'foo')", null),
                    Arguments.of(58, "select * from restaurant where rest_no is null", null),
                    Arguments.of(59, "select * from restaurant where rest_no is not null", null),
                    Arguments.of(60, "select * from restaurant with continuation b64'abc'", null),
                    Arguments.of(61, "select * from restaurant USE INDEX (record_name_idx) where rest_no > 10 ", null),
                    Arguments.of(62, "select * from restaurant USE INDEX (record_name_idx, reviewer_name_idx) where rest_no > 10 ", "Unknown index(es) REVIEWER_NAME_IDX"),
                    Arguments.of(63, "select * from restaurant USE INDEX (record_name_idx), USE INDEX (reviewer_name_idx) where rest_no > 10 ", "Unknown index(es) REVIEWER_NAME_IDX"),
                    Arguments.of(64, "select * from restaurant with continuation", "syntax error[[]]select * from restaurant with continuation[[]]                                          ^^"),
                    Arguments.of(65, "select X.rest_no from (select rest_no from restaurant where 42 >= rest_no OR 42 > rest_no) X", null),
                    Arguments.of(  66, "select X.UNKNOWN from (select rest_no from restaurant where 42 >= rest_no OR 42 > rest_no) X", "Attempting to query non existing column X.UNKNOWN"),
                    Arguments.of(67, "select X.rest_no from (select Y.rest_no from (select rest_no from restaurant where 42 >= rest_no OR 42 > rest_no) Y where 42 >= Y.rest_no OR 42 > Y.rest_no) X", null),
                    Arguments.of(68, "select X.rating from restaurant AS Rec, (select rating from Rec.reviews) X", null),
                    Arguments.of(69, "select COUNT(MAX(Y.rating)) FROM (select rest_no, X.rating from restaurant AS Rec, (select rating from Rec.reviews) X) as Y GROUP BY Y.rest_no", "unsupported nested aggregate(s) count(max_l"),
                    Arguments.of(70, "select rating from restaurant GROUP BY rest_no", "Attempting to query non existing column RATING"),
                    // TODO understand why the query below cannot be planned
                    //Arguments.of(71, "select rating + rest_no, MAX(rest_no) from (select rest_no, X.rating from restaurant AS Rec, (select rating from Rec.reviews) X) as Y GROUP BY rest_no, rating", null),
                    Arguments.of(72, "insert into restaurant_reviewer values (42, \"wrong\", null, null)", "Attempting to query non existing column wrong"),
                    Arguments.of(73, "with recursive c as (with c as (select * from restaurant) select * from c) select * from c", "ambiguous nested recursive CTE name"),
                    Arguments.of(74, "with recursive c as (with c1 as (select * from restaurant) select * from c1) select * from c", null),
                    Arguments.of(75, "with recursive c as (select * from t, c) select * from c", "recursive CTE does not contain non-recursive term"),
                    Arguments.of(76, "with recursive c1 as (select * from restaurant union all select * from c1) select * From c1", null),
                    Arguments.of(77, "with recursive c as (with recursive c1 as (select * from restaurant union all select * from c1) select * From c1 union all select * from restaurant, c) select * from c", null),
                    Arguments.of(78, "with recursive c as (with c as (select * from restaurant) select * from c union all select * from restaurant) select * from c union all select * from restaurant", "ambiguous nested recursive CTE name"),
                    Arguments.of(79, "with recursive c1(name) as (select * from restaurant union all select * from c1) select * From c", "cte query has 7 column(s), however 1 aliases defined")
            );
        }
    }

    @ParameterizedTest(name = "[{0}] {1}")
    @ArgumentsSource(RandomQueryProvider.class)
    void queryTestHarness(int ignored, @Nonnull String query, @Nullable String error) throws Exception {
        final String schemaName = connection.getSchema();
        final EmbeddedRelationalConnection embeddedConnection = (EmbeddedRelationalConnection) connection.connection;
        embeddedConnection.setAutoCommit(false);
        embeddedConnection.createNewTransaction();
        final AbstractDatabase database = embeddedConnection.getRecordLayerDatabase();
        final FDBRecordStoreBase<?> store = database.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(database)
                .fromRecordStore(store, connection.getOptions())
                .withSchemaTemplate(embeddedConnection.getSchemaTemplate())
                .withMetricsCollector(embeddedConnection.getMetricCollector())
                .build();
        if (error == null) {
            PlanGenerator planGenerator = PlanGenerator.create(Optional.empty(), planContext, store, Options.NONE);
            final Plan<?> generatedPlan1 = planGenerator.getPlan(query);
            final var queryHash1 = ((QueryPlan.PhysicalQueryPlan) generatedPlan1).getRecordQueryPlan().semanticHashCode();
            planGenerator = PlanGenerator.create(Optional.empty(), planContext, store, Options.NONE);
            final Plan<?> generatedPlan2 = planGenerator.getPlan(query);
            final var queryHash2 = ((QueryPlan.PhysicalQueryPlan) generatedPlan2).getRecordQueryPlan().semanticHashCode();
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);
            Assertions.assertEquals(queryHash1, queryHash2);
        } else {
            try {
                PlanGenerator planGenerator = PlanGenerator.create(Optional.empty(), planContext, store, Options.NONE);
                planGenerator.getPlan(query);
                Assertions.fail("expected an exception to be thrown");
            } catch (RelationalException e) {
                // there is probably a more intelligent way to do this e.g. via Regex.
                final var parts = error.split(Pattern.quote("[[]]"));
                for (final var part : parts) {
                    assertThat(e.getMessage()).contains(part);
                }
            } catch (Exception e) {
                Assertions.fail("unexpected exception type " + e);
            } finally {
                embeddedConnection.rollback();
                embeddedConnection.setAutoCommit(true);
            }
        }
    }
}

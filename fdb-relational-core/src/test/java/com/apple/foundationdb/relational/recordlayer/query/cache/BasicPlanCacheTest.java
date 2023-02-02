/*
 * BasicPlanCacheTest.java
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.utils.DatabaseRule;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaRule;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;

public class BasicPlanCacheTest {

    private final PlanCache planCache = new ChainedPlanCache(1);

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension(RecordLayerMetadataOperationsFactory::defaultFactory, this::newPlanCache);

    @RegisterExtension
    @Order(1)
    public final SchemaTemplateRule template = new SchemaTemplateRule(relational, "test_plan_cache",
            TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final DatabaseRule db = new DatabaseRule(relational, URI.create("/" + this.getClass().getSimpleName()));

    @RegisterExtension
    @Order(3)
    public final SchemaRule schema = new SchemaRule(relational, "REST", db.getDbUri(), template.getTemplateName());

    @Test
    void planCacheWorks() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:" + db.getDbUri().getPath()), Options.NONE)) {
            conn.setSchema(schema.getSchemaName());
            try (RelationalStatement statement = conn.createStatement()) {
                //verify that the PlanCache is empty
                Assertions.assertThat(planCache.getStats())
                        .returns(0L, CacheStatistics::numEntries)
                        .returns(0L, CacheStatistics::numWrites)
                        .returns(0L, CacheStatistics::numReads)
                        .returns(0L, CacheStatistics::numHits);

                //run a query
                try (RelationalResultSet rs = statement.executeQuery("select name from restaurant")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }

                //now make sure an entry is there
                Assertions.assertThat(planCache.getStats())
                        .returns(1L, CacheStatistics::numEntries)
                        .returns(1L, CacheStatistics::numWrites)
                        .returns(1L, CacheStatistics::numMisses) //there's a single miss the first time we try the query
                        .returns(1L, CacheStatistics::numReads)
                        .returns(0L, CacheStatistics::numHits);

                //re-run the query, it should hit
                try (RelationalResultSet rs = statement.executeQuery("select name from restaurant")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }
                //make sure that the hit was tracked
                Assertions.assertThat(planCache.getStats())
                        .returns(1L, CacheStatistics::numEntries)
                        .returns(1L, CacheStatistics::numWrites)
                        .returns(1L, CacheStatistics::numMisses)
                        .returns(2L, CacheStatistics::numReads)
                        .returns(1L, CacheStatistics::numHits);

            }
        }
    }

    @Test
    void planCacheEvicts() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:" + db.getDbUri().getPath()), Options.NONE)) {
            conn.setSchema(schema.getSchemaName());
            try (RelationalStatement statement = conn.createStatement()) {
                //verify that the PlanCache is empty
                Assertions.assertThat(planCache.getStats())
                        .returns(0L, CacheStatistics::numEntries)
                        .returns(0L, CacheStatistics::numWrites)
                        .returns(0L, CacheStatistics::numReads)
                        .returns(0L, CacheStatistics::numHits);
                //run a query
                try (RelationalResultSet rs = statement.executeQuery("select name from restaurant")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }
                //now make sure an entry is there
                Assertions.assertThat(planCache.getStats())
                        .returns(1L, CacheStatistics::numEntries)
                        .returns(1L, CacheStatistics::numWrites)
                        .returns(1L, CacheStatistics::numMisses) //there's a single miss the first time we try the query
                        .returns(1L, CacheStatistics::numReads)
                        .returns(0L, CacheStatistics::numHits);

                //now run a new query -- this should evict the old entry
                try (RelationalResultSet rs = statement.executeQuery("select rest_no from restaurant")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }

                //running the first query again should miss again
                try (RelationalResultSet rs = statement.executeQuery("select name from restaurant")) {
                    ResultSetAssert.assertThat(rs).hasNoNextRow();
                }
                Assertions.assertThat(planCache.getStats())
                        .returns(1L, CacheStatistics::numEntries)
                        .returns(3L, CacheStatistics::numWrites)
                        .returns(3L, CacheStatistics::numMisses) //1 miss from the first query, 1 miss for the second, and 1 for the second run of the first query
                        .returns(3L, CacheStatistics::numReads)
                        .returns(0L, CacheStatistics::numHits);
            }
        }
    }

    private PlanCache newPlanCache() {
        return planCache;
    }
}

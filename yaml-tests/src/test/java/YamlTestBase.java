/*
 * YamlTestBase.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.server.FRL;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import javax.annotation.Nonnull;
import java.sql.SQLException;

public abstract class YamlTestBase {

    private static final Logger logger = LogManager.getLogger(YamlTestBase.class);

    protected static FRL frl;

    @BeforeAll
    public static void beforeAll() throws RelationalException, SQLException {
        var options = Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 3_600_000L)
                .build();
        frl = new FRL(options);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (frl != null) {
            frl.close();
            frl = null;
        }
    }

    abstract YamlRunner.YamlConnectionFactory createConnectionFactory();

    protected final void doRun(@Nonnull final String fileName) throws Exception {
        doRun(fileName, false);
    }

    protected void doRun(String fileName, boolean correctExplain) throws Exception {
        var yamlRunner = new YamlRunner(fileName, createConnectionFactory(), correctExplain);
        try {
            yamlRunner.run();
        } catch (Exception e) {
            logger.error("‼️ running test file '{}' was not successful", fileName, e);
            throw e;
        }
    }
}

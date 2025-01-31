/*
 * MultiServerFactoryTest.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiServerConnectionFactoryTest {
    @ParameterizedTest
    @CsvSource({"0", "1", "2", "3", "4"})
    void testDefaultPolicy(String initialConnection) throws SQLException {
        int ic = Integer.parseInt(initialConnection);
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.DEFAULT,
                ic,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory()));
        assertEquals(ic, classUnderTest.getCurrentConnection());
        classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(ic, classUnderTest.getCurrentConnection());
        classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(ic, classUnderTest.getCurrentConnection());
    }

    @ParameterizedTest
    @CsvSource({"0", "1", "2", "3", "4"})
    void testAlternatePolicy(String initialConnection) throws SQLException {
        int ic = Integer.parseInt(initialConnection);
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                ic,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory()));
        assertEquals(ic, classUnderTest.getCurrentConnection());
        classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(ic + 1, classUnderTest.getCurrentConnection());
        classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(ic + 2, classUnderTest.getCurrentConnection());
    }

    YamlRunner.YamlConnectionFactory dummyConnectionFactory() {
        return new YamlRunner.YamlConnectionFactory() {
            @Override
            public RelationalConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                return null;
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of("0.0.0.0");
            }
        };
    }
}

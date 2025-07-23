/*
 * YamlConnectionFactoryWithOptions.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Set;

public class YamlConnectionFactoryWithOptions implements YamlConnectionFactory {

    @Nonnull
    private final YamlConnectionFactory underlying;

    @Nonnull
    private final Options options;

    public YamlConnectionFactoryWithOptions(@Nonnull final YamlConnectionFactory underlying, @Nonnull final Options options) {
        this.underlying = underlying;
        this.options = options;
    }

    @Override
    public YamlConnection getNewConnection(@Nonnull final URI connectPath) throws SQLException {
        final var connection = underlying.getNewConnection(connectPath);
        connection.setConnectionOptions(options);
        return connection;
    }

    @Override
    public Set<SemanticVersion> getVersionsUnderTest() {
        return underlying.getVersionsUnderTest();
    }

    @Nonnull
    public static YamlConnectionFactoryWithOptions newInstance(@Nonnull final YamlConnectionFactory underlying,
                                                               @Nonnull final Options options) {
        return new YamlConnectionFactoryWithOptions(underlying, options);
    }
}

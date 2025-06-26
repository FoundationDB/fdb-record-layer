/*
 * EmbeddedSimpleYamlConnection.java
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
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * A simple version of {@link YamlConnection} for interacting with a single {@link RelationalConnection}.
 */
public class EmbeddedSimpleYamlConnection extends SimpleYamlConnection {
    public EmbeddedSimpleYamlConnection(@Nonnull Connection connection, @Nonnull SemanticVersion version) throws SQLException {
        super(connection, version, version.toString());
    }

    public EmbeddedSimpleYamlConnection(@Nonnull Connection connection, @Nonnull SemanticVersion version, @Nonnull String connectionLabel) throws SQLException {
        super(connection, version, connectionLabel);
    }

    @Override
    public void setConnectionOptions(@Nonnull final Options connectionOptions) throws SQLException {
        final RelationalConnection underlying = getUnderlying();
        for (Map.Entry<Options.Name, ?> entry : connectionOptions.entries()) {
            underlying.setOption(entry.getKey(), entry.getValue());
        }
    }
}

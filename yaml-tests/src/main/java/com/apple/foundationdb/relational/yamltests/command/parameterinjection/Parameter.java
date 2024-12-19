/*
 * Parameter.java
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

package com.apple.foundationdb.relational.yamltests.command.parameterinjection;

import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Random;

/**
 * A {@link Parameter} is the internal representation of parsed parameter injection snippet in the query that is
 * provided in the YAML testing framework using the {@link QueryCommand}.
 * <p>
 * Each of the snippet in the given query is translated into some form of (maybe) hierarchical structure of parameters
 * which can then be converted into either:
 * <ul>
 *     <li>A JDBC-compliant SQL object that could be set as parameter to the query when executing using {@link java.sql.PreparedStatement} API.</li>
 *     <li>A constant that replaces snippet in the query itself using {@link java.sql.Statement} API.</li>
 * </ul>
 * The {@link Parameter#getSqlObject} and {@link Parameter#getSqlText} are used respectively for the required conversion.
 * <p>
 * The {@link Parameter} is not guaranteed to produce a literal value always, that is, they can be unbounded. See
 * {@link UnboundParameter} for more information. In case the parameter is unbounded, it first needs to be bound at
 * runtime to fixate to literal values. This can be done using {@link Parameter#bind} method.
 * <p>
 * Here is a textbook usage of this API. Given a query with a snippet,
 * <pre>{@code
 *      SELECT * FROM foo WHERE foo.a > %% one or two or three %%
 *}</pre>
 * %% ... %% defines the snippet. Suppose there is an implementation of {@link Parameter} called {@code TextToInteger}
 * that interprets this snippet. Clearly, {@code TextToInteger} is not bound hence our query doesn't work as it is.
 * Then, calling {@code bind} on this instance of {@code TextToInteger} produces a new bound {@link Parameter} that holds
 * integer literal 1, 2 or 3 (as the snippet describes). The bound result is implementation-specific but is expected to
 * make use of {@link Random} provided to {@link Parameter#bind}. The new bound result can now be used to produce
 * either SQL object or constant that can be used to adapt the query as is needed to be executed.
 */
public interface Parameter {

    /**
     * Binds the parameter to a literal value.
     *
     * @param random the {@link Random} instance used to bind the parameters
     * @return {@code this} if the parameter is already bound, else the bound {@link Parameter}
     */
    @Nonnull
    Parameter bind(@Nonnull Random random);

    /**
     * Returns {@code true} if this parameter is bound, else {@code false}.
     *
     * @return {@code true} if this parameter is bound, else {@code false}
     */
    boolean isUnbound();

    /**
     * Checks whether this parameter is bound and throws {@link UncheckedRelationalException} if it not.
     */
    default void ensureBoundedness() {
        if (isUnbound()) {
            Assert.failUnchecked("The unbounded parameter needs to be first bound");
        }
    }

    /**
     * Return the SQL object for the value of this parameter.
     *
     * @param connection to create complex SQL objects, like {@link Array} and {@link Struct}
     * @return SQL object
     * @throws SQLException if the object creation is not successful
     */
    @Nullable
    Object getSqlObject(@Nullable Connection connection) throws SQLException;

    /**
     * Returns the {@link String} that is the textual SQL representation of the value of this parameter.
     * @return the string
     */
    @Nonnull
    String getSqlText();
}

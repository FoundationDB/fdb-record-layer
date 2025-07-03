/*
 * Transaction.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface Transaction extends AutoCloseable {

    void commit() throws RelationalException;

    void abort() throws RelationalException;

    /**
     * Retrieves an `Optional` containing the bound schema template, if one exists.
     * <br>
     * This method allows access to the schema template that has been bound to this {@link Transaction}.
     * If no schema template is currently bound, an empty {@code Optional} is returned.
     *
     * @return An {@code Optional} containing the bound {@link SchemaTemplate}, or an empty {@link Optional} if no
     * template is bound.
     */
    @Nonnull
    Optional<SchemaTemplate> getBoundSchemaTemplateMaybe();

    /**
     * Sets, or replaces the bound schema template.
     * <br>
     * This method binds the provided {@link SchemaTemplate} to this {@link Transaction}, any subsequent operation
     * that requires interaction with the metadata, within the scope of this {@link Transaction} will use the bound
     * {@link SchemaTemplate} from now on.
     * <br>
     * if there is already a {@link SchemaTemplate} bound to this transaction, it will be replaced by the provided
     * {@link SchemaTemplate} argument.
     * @param schemaTemplate The {@link SchemaTemplate} to bind.  Must not be null.
     */
    void setBoundSchemaTemplate(@Nonnull SchemaTemplate schemaTemplate);

    @Override
    void close() throws RelationalException;

    boolean isClosed();

    /**
     * Unwraps this instance as type T, if such a cast is possible. This provides a convenient API
     * for unwrapping implementation calls from the interface (to avoid lots of instanceof checks everywhere).
     *
     * @param type the type to unwrap it as.
     * @param <T>  the generic type
     * @return this instance, as an instanceof Type T
     * @throws InternalErrorException if instance types are incompatible
     */
    @Nonnull
    default <T> T unwrap(@Nonnull Class<? extends T> type) throws InternalErrorException {
        Class<? extends Transaction> myClass = this.getClass();
        if (myClass.isAssignableFrom(type)) {
            return type.cast(this);
        } else {
            throw new InternalErrorException("Cannot unwrap instance of type <" + myClass.getCanonicalName() + "> as type <" + type.getCanonicalName() + ">");
        }
    }
}

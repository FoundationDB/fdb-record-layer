/*
 * DropTemporaryFunctionConstantAction.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;

public class DropTemporaryFunctionConstantAction implements ConstantAction  {

    private final boolean throwIfNotExists;

    @Nonnull
    private final String temporaryFunctionName;

    public DropTemporaryFunctionConstantAction(boolean throwIfNotExists,
                                               @Nonnull final String temporaryFunctionName) {
        this.throwIfNotExists = throwIfNotExists;
        this.temporaryFunctionName = temporaryFunctionName;
    }

    @Override
    public void execute(final Transaction txn) throws RelationalException {
        final var transactionBoundSchemaTemplate = txn.getBoundSchemaTemplateMaybe();
        if (transactionBoundSchemaTemplate.isPresent()) {
            final var relationalLayerSchemaTemplate = Assert.castUnchecked(transactionBoundSchemaTemplate.orElseThrow(), RecordLayerSchemaTemplate.class);
            final var maybeInvokedRoutine = relationalLayerSchemaTemplate.getInvokedRoutines().stream().filter(r -> r.getName().equals(temporaryFunctionName)).findFirst();
            if (maybeInvokedRoutine.isPresent()) {
                Assert.thatUnchecked(maybeInvokedRoutine.orElseThrow().isTemporary(), ErrorCode.INVALID_FUNCTION_DEFINITION, "Attempt to DROP an non-temporary function: " + temporaryFunctionName);
                final var schemaTemplateWithoutTempFunction = relationalLayerSchemaTemplate.toBuilder().removeInvokedRoutine(temporaryFunctionName).build();
                txn.setBoundSchemaTemplate(schemaTemplateWithoutTempFunction);
                return;
            }
        }
        Assert.thatUnchecked(!throwIfNotExists, ErrorCode.UNDEFINED_FUNCTION, "Attempt to DROP an undefined temporary function: " + temporaryFunctionName);
    }
}

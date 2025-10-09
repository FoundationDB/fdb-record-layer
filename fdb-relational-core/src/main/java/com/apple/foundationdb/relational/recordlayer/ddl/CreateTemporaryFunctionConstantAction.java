/*
 * CreateTemporaryFunctionConstantAction.java
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

import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RoutineParser;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;

public class CreateTemporaryFunctionConstantAction implements ConstantAction  {

    @Nonnull
    private final RecordLayerInvokedRoutine invokedRoutine;

    private final boolean throwIfExists;

    @Nonnull
    private final SchemaTemplate template;

    @Nonnull
    private final PreparedParams preparedParams;

    public CreateTemporaryFunctionConstantAction(@Nonnull final SchemaTemplate template,
                                                 boolean throwIfExists,
                                                 @Nonnull final RecordLayerInvokedRoutine invokedRoutine,
                                                 @Nonnull PreparedParams preparedParams) {
        this.template = template;
        this.throwIfExists = throwIfExists;
        this.invokedRoutine = invokedRoutine;
        this.preparedParams = preparedParams;
    }

    @Override
    public void execute(@Nonnull final Transaction txn) throws RelationalException {
        final var transactionBoundSchemaTemplate = Assert.castUnchecked(txn.getBoundSchemaTemplateMaybe().orElse(template),
                RecordLayerSchemaTemplate.class);

        if (throwIfExists) {
            Assert.thatUnchecked(transactionBoundSchemaTemplate.getInvokedRoutines().stream()
                                    .noneMatch(r -> r.getName().equals(invokedRoutine.getName())),
                    ErrorCode.DUPLICATE_FUNCTION, () -> "function '" + invokedRoutine.getName() + "' already exists");
        }

        // this is to make the temporary function logical plan recreated upon every invocation within the same
        // transaction.
        // this should be simplified once https://github.com/FoundationDB/fdb-record-layer/issues/3394 is fixed.
        final var routineBuilder = invokedRoutine.toBuilder();
        routineBuilder
                .withUserDefinedRoutine(isCaseSensitive ->
                        RoutineParser.sqlFunctionParser(transactionBoundSchemaTemplate)
                        .parseTemporaryFunction(invokedRoutine.getName(), invokedRoutine.getDescription(),
                                PreparedParams.copyOf(preparedParams), isCaseSensitive))
                .withSerializableFunction(new RawSqlFunction(invokedRoutine.getName(), invokedRoutine.getDescription()));
        final var schemaTemplateWithTempFunction = transactionBoundSchemaTemplate.toBuilder()
                .replaceInvokedRoutine(routineBuilder.build()).build();
        txn.setBoundSchemaTemplate(schemaTemplateWithTempFunction);
    }
}

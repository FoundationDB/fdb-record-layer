/*
 * MetadataOperationsFactoryTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataOperationsFactoryTests {

    static Stream<Arguments> operationFactoryMethods() {
        final var dummyURI = URI.create("dummy_uri");
        final var dummyString = "dummy_string";
        final var dummyBoolean = false;
        final var mockedSchemaTemplate = Mockito.mock(SchemaTemplate.class);
        return Stream.of(
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getCreateSchemaConstantAction(dummyURI, dummyString, dummyString, Options.NONE)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getCreateTemporaryFunctionConstantAction(mockedSchemaTemplate, dummyBoolean, Mockito.mock(RecordLayerInvokedRoutine.class))),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getCreateDatabaseConstantAction(dummyURI, Options.NONE)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getSaveSchemaTemplateConstantAction(mockedSchemaTemplate, Options.NONE)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getDropSchemaConstantAction(dummyURI, dummyString, Options.NONE)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getDropSchemaTemplateConstantAction(dummyString, dummyBoolean, Options.NONE)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getDropTemporaryFunctionConstantAction(dummyBoolean, dummyString)),
                Arguments.of((Function<MetadataOperationsFactory, ConstantAction>)factory -> factory.getDropDatabaseConstantAction(dummyURI, dummyBoolean, Options.NONE))
        );
    }

    @ParameterizedTest
    @MethodSource("operationFactoryMethods")
    void abstractMetadataOperationsTest(Function<MetadataOperationsFactory, ConstantAction> getAction) throws Exception {
        final var noOpFactory = NoOpMetadataOperationsFactory.INSTANCE;
        final var abstractFactory = new AbstractMetadataOperationsFactory() {};

        // This is probably not the best way to test the default methods of the abstract class. The minimum it ensures
        // is that the actions are the same - which does not ensures anything about the effects of executing the
        // action.
        Assertions.assertEquals(getAction.apply(noOpFactory), getAction.apply(abstractFactory));
    }

    // This test ensures that the createTemporaryFunction puts the "same" invoked routine, into the transaction-bounded
    // copy of the schema template, that is handed over to it.
    @Test
    void createTemporaryFunctionOperationDoesNotChangeInvokedRoutine() throws RelationalException {
        final var mockedInvokedRoutine = Mockito.mock(RecordLayerInvokedRoutine.class);
        final var mockedUdf = Mockito.mock(UserDefinedFunction.class);
        when(mockedInvokedRoutine.getUserDefinedFunctionProvider()).thenReturn((ignore) -> mockedUdf);
        when(mockedInvokedRoutine.getName()).thenReturn("routine1");
        final var schemaTemplate = RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("Blah")
                        .addColumn(RecordLayerColumn.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Primitives.LONG.type())
                                .build())
                        .build())
                .build();

        final var action = new CreateTemporaryFunctionConstantAction(schemaTemplate, false, mockedInvokedRoutine);

        final var mockedTransaction = Mockito.mock(Transaction.class);
        action.execute(mockedTransaction);

        ArgumentCaptor<RecordLayerSchemaTemplate> captor = ArgumentCaptor.forClass(RecordLayerSchemaTemplate.class);
        verify(mockedTransaction).setBoundSchemaTemplate(captor.capture());
        final var txnBoundSchemaTemplate = captor.getValue();

        Assertions.assertEquals(schemaTemplate.getTables(), txnBoundSchemaTemplate.getTables());
        final var actualRoutineMaybe = txnBoundSchemaTemplate.findInvokedRoutineByName("routine1");
        Assertions.assertTrue(actualRoutineMaybe.isPresent());
        Assertions.assertSame(mockedInvokedRoutine, actualRoutineMaybe.get());
        Assertions.assertSame(((RecordLayerInvokedRoutine) actualRoutineMaybe.get()).getUserDefinedFunctionProvider(), mockedInvokedRoutine.getUserDefinedFunctionProvider());
    }
}

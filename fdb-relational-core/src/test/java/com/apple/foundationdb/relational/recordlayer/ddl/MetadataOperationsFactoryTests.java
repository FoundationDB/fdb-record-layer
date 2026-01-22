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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.net.URI;
import java.util.function.Function;
import java.util.stream.Stream;

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
    void abstractMetadataOperationsTest(Function<MetadataOperationsFactory, ConstantAction> getAction) {
        final var noOpFactory = NoOpMetadataOperationsFactory.INSTANCE;
        final var abstractFactory = new AbstractMetadataOperationsFactory() {};

        // This is probably not the best way to test the default methods of the abstract class. The minimum it ensures
        // is that the actions are the same - which does not ensures anything about the effects of executing the
        // action.
        Assertions.assertEquals(getAction.apply(noOpFactory), getAction.apply(abstractFactory));
    }
}

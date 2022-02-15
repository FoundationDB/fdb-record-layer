/*
 * CreateDbCommand.java
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

package com.apple.foundationdb.relational.cli;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RecordLayerTemplate;

import com.google.protobuf.Descriptors;
import picocli.CommandLine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * Command that creates a database.
 */
@CommandLine.Command(name = "createdb", description = "Creates a database")
public class CreateDbCommand extends Command {

    @CommandLine.Option(names = {"-p", "--path"}, description = "path of the database", required = true)
    String databasePath;
    @CommandLine.Option(names = {"-s", "--schema"}, description = "name of the schema to create", required = true)
    String schemaName;
    @CommandLine.Option(names = {"-st", "--schema-template"}, description = "name of the schema template to use", required = true)
    String schemaTemplateName;

    public CreateDbCommand(DbState dbState) {
        super(dbState, List.of());
    }

    @Override
    public void callInternal() throws Exception {
        String templateId = loadSchemaTemplate(schemaTemplateName);
        try (Transaction txn = new RecordContextTransaction(dbState.getFdbDatabase().openContext())) {
            dbState.getEngine().getConstantActionFactory().getCreateDatabaseConstantAction(URI.create(databasePath), DatabaseTemplate.newBuilder()
                    .withSchema(schemaName, templateId)
                    .build(), Options.create()).execute(txn);
            txn.commit();
        }
    }

    private String loadSchemaTemplate(String schemaTemplate) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String templateId = UUID.randomUUID().toString();
        Method method = Class.forName(schemaTemplate).getDeclaredMethod("getDescriptor");
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords((Descriptors.FileDescriptor) method.invoke(null));
        try (Transaction txn = new RecordContextTransaction(dbState.getFdbDatabase().openContext())) {
            dbState.getEngine().getConstantActionFactory().getCreateSchemaTemplateConstantAction(new RecordLayerTemplate(templateId, builder.build()),
                    Options.create()).execute(txn);
            txn.commit();
        }
        return templateId;
    }
}

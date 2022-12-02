/*
 * JDBCConstantActionFactory.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class JDBCConstantActionFactory implements ConstantActionFactory {
    private final SchemaTemplateCatalog templateCatalog;
    private final RelationalCatalog relationalCatalog;

    public JDBCConstantActionFactory(SchemaTemplateCatalog templateCatalog, RelationalCatalog relationalCatalog) {
        this.templateCatalog = templateCatalog;
        this.relationalCatalog = relationalCatalog;
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        return txn -> templateCatalog.updateTemplate(txn, template.getUniqueId(), template);
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaTemplateConstantAction(@Nonnull String templateId, @Nonnull Options options) {
        return txn -> templateCatalog.deleteTemplate(txn, templateId);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
        return txn -> {
            try {
                Connection conn = getConnection(dbPath);
                conn.close();
            } catch (SQLException e) {
                throw ExceptionUtil.toRelationalException(e);
            }
        };
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
        return txn -> {
            try (Connection sqlConn = getConnection(dbUri)) {
                try (Statement s = sqlConn.createStatement()) {
                    SchemaTemplate template = templateCatalog.loadTemplate(txn, templateId);
                    final DescriptorProtos.FileDescriptorProto fdProto = template.toProtobufDescriptor();
                    Descriptors.FileDescriptor fileDesc = Descriptors.FileDescriptor.buildFrom(fdProto,
                            new Descriptors.FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});

                    Set<Descriptors.Descriptor> tableStructures = new HashSet<>();
                    for (Descriptors.Descriptor typeDesc : fileDesc.getMessageTypes()) {
                        final RecordMetaDataOptionsProto.RecordTypeOptions extension = typeDesc.getOptions().getExtension(RecordMetaDataOptionsProto.record);
                        if (extension.hasUsage() && extension.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION) {
                            //we've found the table listing!
                            for (Descriptors.FieldDescriptor fd : typeDesc.getFields()) {
                                relationalCatalog.loadStructure(fd.getName().replaceFirst("_", ""), fd.getMessageType());
                                tableStructures.add(fd.getMessageType());
                            }
                        }
                    }

                    //now create all the tables
                    relationalCatalog.loadSchema(schemaName, tableStructures);
                    relationalCatalog.createTables(s);

                }
            } catch (SQLException | Descriptors.DescriptorValidationException e) {
                throw ExceptionUtil.toRelationalException(e);
            }
        };
    }

    @Nonnull
    @Override
    public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, @Nonnull Options options) {
        return txn -> {
            try (Connection sqlConn = getConnection(dbUrl)) {
                try (Statement s = sqlConn.createStatement()) {
                    s.execute("DROP ALL OBJECTS DELETE FILES ");
                }
            } catch (SQLException e) {
                throw ExceptionUtil.toRelationalException(e);
            }

        };
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI dbPath, @Nonnull String schemaName, @Nonnull Options options) {
        return txn -> {
            try (Connection sqlConn = getConnection(dbPath)) {
                try (Statement s = sqlConn.createStatement()) {
                    s.execute("DROP SCHEMA " + schemaName);
                }
            } catch (SQLException e) {
                throw ExceptionUtil.toRelationalException(e);
            }
        };
    }

    private Connection getConnection(@Nonnull URI dbPath) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:./src/test/resources/" + dbPath.getPath().replaceAll("/", "_"));
    }
}

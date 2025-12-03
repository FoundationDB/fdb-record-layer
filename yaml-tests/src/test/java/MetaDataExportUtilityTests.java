/*
 * MetaDataExportHelper.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.View;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.RawSqlFunction;
import com.apple.foundationdb.relational.yamltests.generated.identifierstests.IdentifiersTestProto;
import com.apple.foundationdb.relational.yamltests.generated.withdependencies.WithDependenciesProto;
import com.apple.foundationdb.relational.yamltests.utils.ExportSchemaTemplateUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Test utility methods that can be used to set up custom meta-data definitions for YAML tests.
 *
 * <p>
 * The idea here is that we may have a test that wants to control its meta-data more than other tests.
 * For example, it may want to ensure that some choice is made that would be legal for a {@link RecordMetaData}
 * but that the schema template generator may not make. One common case for this is backwards compatibility:
 * if we change the schema template to meta-data code path, we may want to make sure that we can can still deserialize
 * meta-data objects in the old format.
 * </p>
 *
 * <p>
 * To assist with this, there's functionality in the YAML framework that allows the user to specify a JSON file
 * which contains the serialized Protobuf meta-data. These methods allow the user to create a {@link RecordMetaData}
 * using a {@link RecordMetaDataBuilder} or some other method, and then create
 * such a JSON file. The user should then commit the output of running these tests. If an existing test file
 * needs to be updated, it is usually the case that the user should update this file and regenerate the meta-data
 * rather than manually update the file.
 * </p>
 */
@Disabled("for updating test files that should be checked in")
class MetaDataExportUtilityTests {

    private static void exportMetaData(@Nonnull RecordMetaData metaData, @Nonnull String name) throws IOException {
        Path path = Path.of("src", "test", "resources", name);
        ExportSchemaTemplateUtil.export(metaData, path);
    }

    private void setAllPrimaryKeys(@Nonnull RecordMetaDataBuilder metaDataBuilder, @Nonnull KeyExpression primaryKey) {
        metaDataBuilder.getUnionDescriptor().getFields().forEach( f -> {
            final String typeName = f.getMessageType().getName();
            metaDataBuilder.getRecordType(typeName).setPrimaryKey(primaryKey);
        });
    }

    @Test
    void createIncludedDependenciesMetaData() throws IOException {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(WithDependenciesProto.getDescriptor());
        setAllPrimaryKeys(metaDataBuilder, Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("id")));

        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("T"), new Index("T$x", Key.Expressions.field("x").nest(Key.Expressions.concatenateFields("a", "b"))));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("U"), new Index("U$x", Key.Expressions.field("x").nest(Key.Expressions.concatenateFields("a", "b"))));

        final RecordMetaData metaData = metaDataBuilder.build();

        // Modify the proto file names so that they aren't available in the classpath.
        // This ensures that the test case validates that we load the dependency from the
        // protobuf serialized MetaData proto and not from the environment.
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.toProto().toBuilder();
        protoBuilder.getRecordsBuilder()
                .setName("modified_" + protoBuilder.getRecordsBuilder().getName())
                .clearDependency()
                .addDependency("modified_to_be_imported.proto");
        protoBuilder.getDependenciesBuilder(0)
                .setName("modified_" + protoBuilder.getDependenciesBuilder(0).getName());
        final RecordMetaData modified = RecordMetaData.build(protoBuilder.build());

        exportMetaData(modified, "import-schema-template/with_included_dependencies_metadata.json");
    }

    @Test
    void createValidIdentifiersMetaData() throws IOException {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(IdentifiersTestProto.getDescriptor());
        setAllPrimaryKeys(metaDataBuilder, Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("ID")));

        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("T2"), new Index("T2$T2.COL1", "T2__1COL1"));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("___T6__2__UNESCAPED"), new Index("T6$COL2", "__T6__2COL2__VALUE"));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("___T6__2__UNESCAPED"), new Index("T6$ENUM2", "T6__1__ENUM_2"));
        metaDataBuilder.addUserDefinedFunction(new RawSqlFunction("__func__T3$col2",
                "CREATE FUNCTION \"__func__T3$col2\"(in \"x$\" bigint) AS select \"__T3$COL1\" as \"c.1\", \"__T3$COL3\" as \"c.2\" from \"__T3\" WHERE \"__T3$COL2\" = \"x$\""));
        metaDataBuilder.addView(new View("T4$view",
                "select \"T4.COL1\" AS \"c__1\", \"T4.COL2\" AS \"c__2\" from T4 where \"T4.COL1\" > 0 and \"T4.COL2\" > 0"));
        final RecordMetaData metaData = metaDataBuilder.build();
        exportMetaData(metaData, "valid_identifiers_metadata.json");
    }
}

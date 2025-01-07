/*
 * SetSchema.java
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

package sqlline;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import org.jline.reader.Completer;

import java.sql.SQLException;
import java.util.Collections;

/**
 * Set Relational schema.
 */
// This command is in same package as sqlline to get access to package private resources.

@SuppressWarnings("PMD.GuardLogStatement")
@API(API.Status.EXPERIMENTAL)
public class SetSchema extends AbstractCommandHandler {
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public SetSchema(SqlLine sqlLine) {
        super(sqlLine, new String[]{"setschema", "ss"}, "Set Relational Schema: e.g. !ss <SCHEMA_NAME>.",
                Collections.<Completer>emptyList());
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
        String schemaName = Utils.stripCommandName(line, getNames());
        if (schemaName == null) {
            sqlLine.error("Expects `setschema <SCHEMA_NAME>`;" +
                    "failed to parse a schema name from " + line);
            dispatchCallback.setToFailure();
            return;
        }
        try {
            this.sqlLine.getDatabaseConnection().getConnection().setSchema(schemaName);
            dispatchCallback.setToSuccess();
        } catch (SQLException e) {
            dispatchCallback.setToFailure();
            this.sqlLine.handleException(e);
        }
    }
}

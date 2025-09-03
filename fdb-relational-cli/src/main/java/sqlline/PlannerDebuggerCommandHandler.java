/*
 * PlannerDebuggerCommandHandler.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.PlannerRepl;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import org.jline.reader.Completer;

import java.util.Collections;

/**
 * Add a sqlline command that will launch the cascades planner debugger in side sqlline.
 * This new command shows in the general list of commands when you do '!help' on the
 * `sqlline` commandline. You trigger this command by executing '!plannerdebugger SQL`
 * (or '!pd SQL') where SQL is the SQL you want to run under the debugger:
 * e.g. <code>!pd select * from databases;</code>.
 */
// This command is in same package as sqlline to get access to package private resources.

@SuppressWarnings("PMD.GuardLogStatement")
@API(API.Status.EXPERIMENTAL)
public class PlannerDebuggerCommandHandler extends AbstractCommandHandler {
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public PlannerDebuggerCommandHandler(SqlLine sqlLine) {
        super(sqlLine, new String []{"plannerdebugger", "pd"},
                "Launch Cascades Planner debugger: e.g. !pd <SQL_TO_DEBUG>",
                Collections.<Completer>emptyList());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public void execute(String line, DispatchCallback dispatchCallback) {
        if (sqlLine.getConnectionMetadata() == null || sqlLine.getConnectionMetadata().getUrl() == null) {
            // Must be the embedded jdbc driver if want to launch the debugger.
            sqlLine.error("The Cascades PlannerDebugger requires a database Connection; connect and then retry.");
            dispatchCallback.setToFailure();
            return;
        }
        if (!sqlLine.getConnectionMetadata().getUrl().startsWith("jdbc:embed:")) {
            // Must be the embedded jdbc driver if want to launch the debugger.
            sqlLine.error("The Cascades PlannerDebugger works in a jdbc:embed: JDBC Driver context only!");
            dispatchCallback.setToFailure();
            return;
        }
        Debugger.setDebugger(new PlannerRepl(this.sqlLine.getTerminal(), false));
        Debugger.setup();
        String sql = Utils.stripCommandName(line, getNames());
        if (sql == null) {
            sqlLine.error("Pass the Cascades PlannerDebugger SQL to run: e.g. `!pd select * from databases;`," +
                    "not " + line);
            dispatchCallback.setToFailure();
            return;
        }
        try {
            // Run the SQL under the debugger.
            this.sqlLine.dispatch(sql, dispatchCallback);
        } catch (Exception e) {
            dispatchCallback.setToFailure();
            sqlLine.error(e);
        } finally {
            Debugger.setDebugger(null);
        }
    }
}

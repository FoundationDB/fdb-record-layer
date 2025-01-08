/*
 * Customize.java
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

package com.apple.foundationdb.relational.cli.sqlline;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import sqlline.BuiltInProperty;
import sqlline.CommandHandler;
import sqlline.OutputFormat;
import sqlline.PlannerDebuggerCommandHandler;
import sqlline.SetSchema;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In here we add Relational customizations and extensions to SQLline. See
 * <a href="https://github.com/julianhyde/sqlline/blob/main/src/main/java/sqlline/Application.java#L38">Application.java</a>
 * on how this works. The main thing we add is output of Relational STRUCT and ARRAY missing from basic sqlline.
 */
@API(API.Status.EXPERIMENTAL)
public class Customize extends sqlline.Application {
    // Overriding {@link #getDefaultInteractiveMode()}, and {@link #getConnectInteractiveModes()} doesn't work -- bug
    // because sqlline does this <code>new HashSet<>(new Application().getConnectInteractiveModes()))</code> -- so we have
    // an ugly workaround in the below to avoid folks have to supply name and password every time (the default).

    /**
     * Override to fix a bug in sqlline customization; an oversight prevents us being able to set the
     * CONNECT_INTERACTION_MODE default so we do the below ugly intercept instead. Relational does not support
     * login yet so do the below to stop user being challenged for a username/password on each connect.
     * @return Our options instance.
     */
    @Override
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public SqlLineOpts getOpts(SqlLine sqlLine) {
        // Set do-not-ask-for-login credentials -- login not supported on Relational, not yet.
        sqlLine.getOpts().set(BuiltInProperty.CONNECT_INTERACTION_MODE, "notAskCredentials");
        return sqlLine.getOpts();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public String getInfoMessage() {
        // Prepend 'Relational' to hint Relational context.
        return "Relational " + getVersion();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public Map<String, OutputFormat> getOutputFormats(SqlLine sqlLine) {
        //        final Map<String, OutputFormat> outputFormats = new HashMap<>();
        //        outputFormats.put("vertical", new VerticalOutputFormat(sqlLine));
        //        outputFormats.put("table", new TableOutputFormat(sqlLine));
        //        outputFormats.put("ansiconsole", new AnsiConsoleOutputFormat(sqlLine));
        //        outputFormats.put("csv", new SeparatedValuesOutputFormat(sqlLine, ","));
        //        outputFormats.put("tsv", new SeparatedValuesOutputFormat(sqlLine, "\t"));
        //        XmlAttributeOutputFormat xmlAttrs = new XmlAttributeOutputFormat(sqlLine);
        //        // leave "xmlattr" name for backward compatibility,
        //        // "xmlattrs" should be used instead
        //        outputFormats.put("xmlattr", xmlAttrs);
        //        outputFormats.put("xmlattrs", xmlAttrs);
        //        outputFormats.put("xmlelements", new XmlElementOutputFormat(sqlLine));
        // return Collections.unmodifiableMap(outputFormats);

        // Make a copy of the unmodifiable Map into a Map we can modify.
        Map<String, OutputFormat> copy = new HashMap<>(super.getOutputFormats(sqlLine));
        // Overwrite the json implementation with ours.
        copy.put("json", new JsonOutputFormatPlus(sqlLine));
        // Now return unmodifiable Map like our super method does.
        return Collections.unmodifiableMap(copy);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport // Hard to make a test that makes sense given this an sqlline internal.
    public Collection<CommandHandler> getCommandHandlers(SqlLine sqlLine) {
        // Make a copy of super handlers list to get around the super making the list unmodifiable.
        List<CommandHandler> handlers = new ArrayList<>(super.getCommandHandlers(sqlLine));
        handlers.add(new PlannerDebuggerCommandHandler(sqlLine));
        handlers.add(new SetSchema(sqlLine));
        // Make is unmodifiable again.
        return Collections.unmodifiableList(handlers);
    }
}

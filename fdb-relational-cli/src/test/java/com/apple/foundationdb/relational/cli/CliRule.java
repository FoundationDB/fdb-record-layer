/*
 * CliRule.java
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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

public class CliRule implements BeforeEachCallback, AfterEachCallback {
    private StringWriter output;
    private StringWriter error;

    private CliManager cliManager;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        output = new StringWriter();
        error = new StringWriter();
        cliManager = new CliManager(new PrintWriter(output), new PrintWriter(error));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        cliManager.close();
    }

    public CommandLine getCmd() {
        return cliManager.getCommandLine();
    }

    public DbState getDbState() {
        return cliManager.getDbState();
    }

    public String getOutput() {
        return output.toString();
    }

    public PrintWriter getOutputWriter() {
        return new PrintWriter(output);
    }

    public String getError() {
        return error.toString();
    }
}

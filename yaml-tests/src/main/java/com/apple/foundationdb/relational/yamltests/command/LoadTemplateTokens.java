/*
 * LoadTemplateTokens.java
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

package com.apple.foundationdb.relational.yamltests.command;

import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * The class holds parsed tokens in load schema template commands.
 */
public class LoadTemplateTokens {
    private final String templateName;
    private final String jsonFileName;
    @Nonnull
    private final List<String> dependencies;

    private LoadTemplateTokens(final String templateName, final String jsonFileName, @Nonnull final List<String> dependencies) {
        this.templateName = templateName;
        this.jsonFileName = jsonFileName;
        this.dependencies = dependencies;
    }

    public static LoadTemplateTokens of(String loadCommandString) {
        StringTokenizer lcsTokenizer = new StringTokenizer(loadCommandString, " ");
        String templateName = lcsTokenizer.nextToken();
        if (!"from".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load command looking like X from Y");
        }
        String jsonFileName = lcsTokenizer.nextToken();
        List<String> dependencies = new ArrayList<>();
        while (lcsTokenizer.hasMoreTokens()) {
            dependencies.add(lcsTokenizer.nextToken());
        }
        return new LoadTemplateTokens(templateName, jsonFileName, dependencies);
    }

    public String getTemplateName() {
        return templateName;
    }

    public String getJsonFileName() {
        return jsonFileName;
    }

    @Nonnull
    public List<String> getDependencies() {
        return dependencies;
    }
}

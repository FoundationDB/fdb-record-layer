/*
 * SupportedVersion.java
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

package com.apple.foundationdb.relational.yamltests.server;

import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.block.FileOptions;

import java.util.List;
import java.util.Set;

/**
 * Class to support the various places in a yaml file where you can have supported_version.
 */
public class SupportedVersionCheck {

    private final boolean isSupported;
    private final String message;

    private SupportedVersionCheck() {
        isSupported = true;
        message = "";
    }

    private SupportedVersionCheck(final String message) {
        isSupported = false;
        this.message = message;
    }

    public static SupportedVersionCheck parse(Object rawVersion, YamlExecutionContext executionContext) {
        if (rawVersion instanceof FileOptions.CurrentVersion) {
            final Set<String> versionsUnderTest = executionContext.getConnectionFactory().getVersionsUnderTest();
            // IntelliJ, at least, doesn't display the reason, so log it
            if (!versionsUnderTest.isEmpty()) {
                return new SupportedVersionCheck(
                        "Skipping test that only works against the current version, when we're running with these versions: " +
                                versionsUnderTest);
            }
            return new SupportedVersionCheck();
        } else if (rawVersion instanceof String) {
            final SemanticVersion supported = SemanticVersion.parse((String)rawVersion);
            final List<SemanticVersion> unsupportedVersions = supported.lesserVersions(
                    executionContext.getConnectionFactory().getVersionsUnderTest());
            if (!unsupportedVersions.isEmpty()) {
                return new SupportedVersionCheck("Skipping test that only works against " + supported +
                        " and later, but we are running with these older versions: " + unsupportedVersions);
            }
            return new SupportedVersionCheck();
        } else {
            throw new RuntimeException("Unsupported supported_version: " + rawVersion);
        }
    }

    public boolean isSupported() {
        return isSupported;
    }

    public String getMessage() {
        return message;
    }
}

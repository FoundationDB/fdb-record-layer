/*
 * SupportedVersionCheck.java
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
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to support the various places in a yaml file where you can have supported_version.
 */
public class SupportedVersionCheck {
    public static final String SUPPORTED_VERSION_OPTION = "supported_version";
    public static final Set<String> OPTION_KEYS = ImmutableSet.of(SUPPORTED_VERSION_OPTION);
    public static SupportedVersionCheck SUPPORTED = new SupportedVersionCheck(true, "");

    private final boolean isSupported;
    private final String message;

    private SupportedVersionCheck(boolean isSupported, String message) {
        this.isSupported = isSupported;
        this.message = message;
    }

    public static SupportedVersionCheck parseOptions(Map<?, ?> options, YamlExecutionContext executionContext) {
        var builder = SupportedVersionCheck.newBuilder(executionContext);
        for (Map.Entry<?, ?> entry : options.entrySet()) {
            builder = builder.consumeLine(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private static SupportedVersionCheck parse(Object rawVersion, YamlExecutionContext executionContext) {
        if (rawVersion instanceof FileOptions.CurrentVersion) {
            executionContext.getConnectionFactory().getVersionsUnderTest();
            final Set<String> versionsUnderTest = executionContext.getConnectionFactory().getVersionsUnderTest();
            // IntelliJ, at least, doesn't display the reason, so log it
            if (versionsUnderTest.stream().anyMatch(v -> !v.equals(FileOptions.CurrentVersion.TEXT))) {
                return SupportedVersionCheck.unsupported(
                        "Skipping test that only works against the current version, when we're running with these versions: " +
                                versionsUnderTest);
            }
            return supported();
        } else if (rawVersion instanceof String) {
            final SemanticVersion supported = SemanticVersion.parse((String)rawVersion);
            final List<SemanticVersion> unsupportedVersions = supported.lesserVersions(
                    executionContext.getConnectionFactory().getVersionsUnderTest());
            if (!unsupportedVersions.isEmpty()) {
                return SupportedVersionCheck.unsupported("Skipping test that only works against " + supported +
                        " and later, but we are running with these older versions: " + unsupportedVersions);
            }
            return supported();
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

    public static Builder newBuilder(@Nonnull YamlExecutionContext executionContext) {
        return new Builder(executionContext);
    }

    public static SupportedVersionCheck supported() {
        return SUPPORTED;
    }

    public static SupportedVersionCheck unsupported(@Nonnull String message) {
        return new SupportedVersionCheck(false, message);
    }

    public static class Builder {
        private final YamlExecutionContext executionContext;
        @Nullable
        private Object rawVersion;

        private Builder(YamlExecutionContext executionContext) {
            this.executionContext = executionContext;
        }

        @Nonnull
        public Builder setRawVersion(final Object rawVersion) {
            this.rawVersion = rawVersion;
            return this;
        }

        @Nonnull
        public Builder consumeLine(final Object key, final Object value) {
            if (SUPPORTED_VERSION_OPTION.equals(key)) {
                return setRawVersion(value);
            }
            return this;
        }

        @Nonnull
        public SupportedVersionCheck build() {
            if (rawVersion == null) {
                return supported();
            }
            return parse(rawVersion, executionContext);
        }
    }
}

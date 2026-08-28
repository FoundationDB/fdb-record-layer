/*
 * FDBTestEnvironment.java
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

package com.apple.foundationdb.test;

import org.assertj.core.api.Assumptions;
import org.yaml.snakeyaml.Yaml;

import org.jspecify.annotations.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for accessing {@code fdb-environment.yaml}, so that tests can know about fdb clusters
 * that are available.
 */
public final class FDBTestEnvironment {
    // A cluster file entry may be null, meaning "use the default cluster file" (no fdb-environment.yaml
    // configured, or an environment entry that intentionally leaves the cluster file unspecified).
    private static final List<@Nullable String> clusterFiles;

    static {
        final String fdbEnvironment = System.getenv("FDB_ENVIRONMENT_YAML");

        if (fdbEnvironment != null && !fdbEnvironment.isEmpty()) {
            clusterFiles = List.copyOf(parseFDBEnvironmentYaml(fdbEnvironment));
        } else {
            clusterFiles = Collections.singletonList(null); // List.of does not allow null
        }
    }

    private FDBTestEnvironment() {
    }

    @SuppressWarnings("unchecked")
    private static List<String> parseFDBEnvironmentYaml(final String fdbEnvironment) {
        Yaml yaml = new Yaml();
        try (FileInputStream yamlInput = new FileInputStream(fdbEnvironment)) {
            Object fdbConfig = yaml.load(yamlInput);
            List<String> parsedClusterFiles = (List<String>)((Map<?, ?>)fdbConfig).get("clusterFiles");
            if (parsedClusterFiles == null) {
                throw new IllegalStateException("fdb-environment.yaml at " + fdbEnvironment + " does not define \"clusterFiles\"");
            }
            return parsedClusterFiles;
        } catch (IOException e) {
            throw new IllegalStateException("Could not read fdb-environment.yaml", e);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Could not parse fdb environment file " + fdbEnvironment, e);
        }
    }

    @Nullable
    public static String getClusterFile(int i) {
        return clusterFiles.get(i);
    }

    public static List<@Nullable String> allClusterFiles() {
        return clusterFiles;
    }

    public static List<@Nullable String> allClusterFilesInRandomOrder() {
        final List<@Nullable String> randomized = new ArrayList<>(clusterFiles);
        Collections.shuffle(randomized);
        return randomized;
    }

    @Nullable
    public static String randomClusterFile() {
        return clusterFiles.get(ThreadLocalRandom.current().nextInt(clusterFiles.size()));
    }

    /**
     * Marks the current test as skipped if there are not the desired number of clusters available to test against.
     * @param desiredCount the number of clusters to test against
     */
    public static void assumeClusterCount(final int desiredCount) {
        if (desiredCount > 1) {
            Assumptions.assumeThat(clusterFiles.size()).as("Cluster file count").isGreaterThanOrEqualTo(desiredCount);
        }
    }
}

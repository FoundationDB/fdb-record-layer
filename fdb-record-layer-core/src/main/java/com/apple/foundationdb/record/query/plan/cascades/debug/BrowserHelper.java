/*
 * BrowserHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

import javax.annotation.Nonnull;
import java.awt.Desktop;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class to interface with the system browser.
 */
public class BrowserHelper {

    private BrowserHelper() {
        // shut up intellij
    }

    /**
     * For debugging only! This method locates a template html launcher file from the resources folder and writes a
     * specific launcher html file into a temp directory. If wanted the caller can open the html in a browser of
     * choice.
     *
     * @param htmlAsResource path to a resource containing an html template
     * @param insertMap a map from a placeholder in the template resource to a string to be replaced with something
     *        dynamic
     * @return a URI pointing to a html file in a temp location which renders the graph
     * @throws Exception -- thrown from methods called in here.
     */
    @Nonnull
    private static URI createHtmlLauncher(@Nonnull final String htmlAsResource,
                                          @Nonnull final Map<String, String> insertMap) throws Exception {
        String launcherHtmlString;
        try (InputStream launcherHtmlInputStream = BrowserHelper.class.getResourceAsStream(htmlAsResource)) {
            launcherHtmlString = CharStreams.toString(new InputStreamReader(Objects.requireNonNull(launcherHtmlInputStream), StandardCharsets.UTF_8));
            // definitely no the most performant way of doing this
            for (final Map.Entry<String, String> entry : insertMap.entrySet()) {
                launcherHtmlString = launcherHtmlString.replace(entry.getKey(), entry.getValue());
            }
        }
        final File launcherTempFile = File.createTempFile("local_launcher-", ".html", new File(System.getProperty("java.io.tmpdir")));
        final String launcherTempFileName = launcherTempFile.toString();
        try {
            try (PrintWriter writer = new PrintWriter(launcherTempFile, StandardCharsets.UTF_8)) {
                writer.print(launcherHtmlString);
            }
        } catch (Exception e) {
            throw new Exception("Error writing file: " + launcherTempFile.getAbsolutePath() + " : " + e.getMessage(), e);
        }
        return new URI("file:///" + launcherTempFileName.replace("\\", "/"));
    }

    /**
     * Show the planner expression that is passed in as a graph rendered in your default browser.
     * @param htmlAsResource path to a resource containing an html template
     * @param insertMap a map from a placeholder in the template resource to a string to be replaced with something
     *        dynamic
     * @return the word "done" (IntelliJ really likes a return of String).
     */
    @Nonnull
    public static String browse(@Nonnull final String htmlAsResource,
                                @Nonnull final Map<String, String> insertMap) {
        try {
            final URI uri = BrowserHelper.createHtmlLauncher(htmlAsResource, insertMap);
            Desktop.getDesktop().browse(uri);
            return "done";
        } catch (final Exception ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }
}

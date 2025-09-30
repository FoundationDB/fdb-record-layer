/*
 * WorkloadReporter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest.engine;

import org.opentest4j.TestAbortedException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

class WorkloadReporter {
    private final String reportDirectory;
    private String fileName;

    private Properties properties;
    private String workloadName;
    private long timestamp;
    private long timeMillis;

    private final Queue<TestReporter> reporters = new ConcurrentLinkedQueue<>();

    public WorkloadReporter(String reportDirectory) throws IOException {
        this.reportDirectory = reportDirectory;

        //make sure the directories exist
        Files.createDirectories(Path.of(reportDirectory));
    }

    public void publishWorkload(AutoWorkload workload) {
        fileName = "AutoworkloadTest" + workload.getDisplayName().replaceAll("/", ".");
        workloadName = workload.getDisplayName();
        properties = new Properties();
        properties.put("SchemaDescription", workload.getSchema());
        properties.put("seed", workload.getConfig().getSeed());
        properties.put("reportDirectory", workload.getConfig().getReportDirectory());
        properties.putAll(workload.getConfig().asMap());
        timestamp = System.currentTimeMillis();
    }

    public void publishProperty(String key, String balue) {
        properties.put(key, balue);
    }

    public TestReporter beginTest(String testName) {
        TestReporter wtr = new TestReporter(testName);
        reporters.add(wtr);
        return wtr;
    }

    public void workloadComplete() throws IOException {
        timeMillis = System.currentTimeMillis() - timestamp;

        printResults();
    }

    public void printResults() throws IOException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element root = doc.createElement("testsuite");

            root.setAttribute("name", workloadName);
            root.setAttribute("tests", Integer.toString(reporters.size()));
            root.setAttribute("failures", Long.toString(reporters.stream()
                    .filter(TestReporter::isFailure).count()));
            root.setAttribute("errors", Long.toString(reporters.stream()
                    .filter(TestReporter::isError).count()));
            root.setAttribute("skipped", Long.toString(reporters.stream()
                    .filter(TestReporter::isSkipped).count()));
            root.setAttribute("timestamp", SimpleDateFormat.getDateTimeInstance().format(new Date(timestamp)));
            root.setAttribute("time", printTime(timeMillis));

            Element propsElement = doc.createElement("properties");
            root.appendChild(propsElement);

            properties.forEach((key, value) -> {
                Element property = doc.createElement("property");
                property.setAttribute("name", key.toString());
                property.setAttribute("value", value.toString());
                propsElement.appendChild(property);
            });

            doc.appendChild(root);

            Element testElem = doc.createElement("tests");
            for (TestReporter report : reporters) {
                Element testChildElement = doc.createElement("testcase");
                testChildElement.setAttribute("name", report.getTestName());
                testChildElement.setAttribute("time", printTime(report.runtime));
                if (report.isSkipped()) {
                    final Element skipped = doc.createElement("skipped");
                    skipped.setNodeValue("true");
                    testChildElement.appendChild(skipped);
                } else if (report.isError()) {
                    final Element error = doc.createElement("error");
                    error.setAttribute("type", report.failure.getClass().getSimpleName());
                    error.setAttribute("message", report.failure.toString());
                    testChildElement.appendChild(error);
                } else if (report.isFailure()) {
                    final Element failure = doc.createElement("failure");
                    failure.setAttribute("type", report.failure.getClass().getSimpleName());
                    StringWriter sw = new StringWriter();
                    report.failure.printStackTrace(new PrintWriter(sw));
                    failure.setAttribute("message", report.failure.getMessage());
                    failure.setTextContent(sw.toString());
                    testChildElement.appendChild(failure);
                }

                if (!report.addedContext.isEmpty()) {
                    Element contextElem = doc.createElement("properties");

                    report.addedContext.forEach(new BiConsumer<String, String>() {
                        @Override
                        public void accept(String key, String value) {
                            Element ctxElem = doc.createElement("property");
                            ctxElem.setAttribute("name", key);
                            ctxElem.setAttribute("value", "CDATA[" + value + "]");
                            contextElem.appendChild(ctxElem);
                        }
                    });
                    testChildElement.appendChild(contextElem);
                }
                testElem.appendChild(testChildElement);
            }
            root.appendChild(testElem);

            try (OutputStream os = Files.newOutputStream(Path.of(reportDirectory, fileName))) {
                TransformerFactory tFactory = TransformerFactory.newInstance();
                Transformer transformer = tFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                DOMSource source = new DOMSource(doc);
                StreamResult result = new StreamResult(os);

                transformer.transform(source, result);
                os.flush();
            }

        } catch (ParserConfigurationException | TransformerException e) {
            throw new IOException(e);
        }
    }

    private static String printTime(long runtimeMillis) {
        long seconds = runtimeMillis / 1000;
        runtimeMillis -= seconds * 1000;
        long minutes = seconds / 60;
        seconds -= minutes * 60;
        long hours = minutes / 60;
        minutes -= hours * 60;
        String durationString = "";
        if (hours > 0) {
            durationString += hours + "h";
        }
        if (minutes > 0) {
            durationString += minutes + "m";
        }
        durationString += seconds + "." + runtimeMillis;

        return durationString;
    }

    //A separate reporter used to create special contextual elements for each test specifically.
    public static class TestReporter implements org.junit.jupiter.api.TestReporter {
        private final String testName;
        private final long timestamp = System.currentTimeMillis();

        private Throwable failure;

        private long runtime;
        private boolean skipped;
        private boolean setupSuccess;

        private final Map<String, String> addedContext = new HashMap<>();

        public TestReporter(String testName) {
            this.testName = testName;
        }

        @Override
        public void publishEntry(Map<String, String> map) {
            addedContext.putAll(map);
        }

        public void testFailed(Throwable t, boolean duringSetup) {
            this.runtime = System.currentTimeMillis() - timestamp;
            this.failure = t;
            if (t instanceof TestAbortedException) {
                skipped = true;
            }
            this.setupSuccess = !duringSetup;
        }

        public void testSuccess() {
            this.runtime = System.currentTimeMillis() - timestamp;
        }

        public boolean isFailure() {
            return failure != null && !skipped && setupSuccess;
        }

        public boolean isError() {
            return failure != null && !skipped && !setupSuccess;
        }

        public boolean isSkipped() {
            return skipped;
        }

        public String getTestName() {
            return testName;
        }
    }
}

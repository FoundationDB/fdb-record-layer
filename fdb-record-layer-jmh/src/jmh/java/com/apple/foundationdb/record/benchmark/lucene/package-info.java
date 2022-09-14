/*
 * package-info.java
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

package com.apple.foundationdb.record.benchmark.lucene;
/**
 * Running the benchmarks:
 *
 * To run a specific benchmark through JMH, you can either
 *
 * A. configure IDEA to run them, if you want to run it in your IDE
 * B. run them using gradle.
 *
 * Running with IDE
 * You'll need to manually add the contents of <em>fdb-environment.properties</em> to the env variables
 * of the run configuration to make it work. Otherwise, just call the main method inside the benchmark class file
 *
 * Note, however, that running the benchmark inside of an IDE is generally going to give you iffy results--you'll
 * get more reliable results running through Gradle
 *
 * Running with Gradle
 * If you want to run the benchmark using gradle, do the following:
 *
 * gradle fdb-record-layer-lucene:jmh -Pargs="[nameOfBenchmark].*"
 *
 * If you want to attach async-profiler, do:
 * gradle -Djava.library.path=[pathToAsyncProfiler] fdb-record-layer-lucene:jmh -Pargs="...-prof async"
 *
 * To make async-profiler store data in a flamegraph:
 *
 * gradle -Djava.library.path=[pathToAsyncProfiler] fdb-record-layer-lucene:jmh -Pargs="...-prof async:output=flamegraph"
 *
 * other options are available,consult {@link org.openjdk.jmh.profile.AsyncProfiler} for details.
 *
 * Loading data:
 * To load data, put all the emails into the "fdb-record-layer-lucene/src/test/resources/mail_test_data" directory,
 *  and then use the main method in {@link com.apple.foundationdb.record.benchmark.lucene.MailDataLoader} to load the data.
 *
 * If your data is not in the format that MailDataLoader expects, then we will need to modify and/or generify the loader
 * logic to make that work.
 */

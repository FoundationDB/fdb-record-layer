/*
 * MailUpdateBench2.java
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

import com.apple.foundationdb.record.TestMailProto;
import com.apple.foundationdb.record.benchmark.BenchmarkTimer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 60)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MailUpdateBench {

    @State(Scope.Thread)
    public static class InsertMessage {
        private TestMailProto.TextSearch textSearch;
        private TestMailProto.MessageState messageState;

        private Random random = new Random(0L);

        @Setup(Level.Invocation)//all these benchmarks are easily slow enough to make this level aceptable
        public void findMessage(LuceneConfiguration benchCfg,
                                MailQueryBench.Noncovering nonCovered) throws Exception {
            int nextRecId = random.nextInt(benchCfg.numRecords);
            int nextUid = -1 * nextRecId;

            nonCovered.run(new BenchmarkTimer(), store -> {
                Message msgState = store.loadRecord(Tuple.from(nextUid)).getRecord();
                if (msgState instanceof DynamicMessage) {
                    TestMailProto.MessageState.Builder msgStateBuilder = TestMailProto.MessageState.newBuilder();
                    msgState.getAllFields().forEach((field, value) -> {
                        msgStateBuilder.setField(TestMailProto.MessageState.getDescriptor().findFieldByName(field.getName()), value);
                    });
                    messageState = msgStateBuilder.build();
                }
                Message tSearch = store.loadRecord(Tuple.from(nextRecId)).getRecord();
                if (tSearch instanceof DynamicMessage) {
                    TestMailProto.TextSearch.Builder txtSearchBuilder = TestMailProto.TextSearch.newBuilder();
                    tSearch.getAllFields().forEach((field, value) -> {
                        txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName(field.getName()), value);
                    });
                    textSearch = txtSearchBuilder.build();
                }
            });
        }
    }

    @Benchmark
    public void insertSingleRecordNoncoveredIndex(LuceneConfiguration config,
                                                  MailQueryBench.Noncovering fdbCfg,
                                                  BenchmarkTimer timer,
                                                  InsertMessage msg) {

        fdbCfg.run(timer, store -> {

            store.saveRecord(msg.messageState);
            store.saveRecord(msg.textSearch);

        });
    }

    @Benchmark
    public void insertSingleRecordCoveredIndex(LuceneConfiguration config,
                                                  MailQueryBench.Covering fdbCfg,
                                                  BenchmarkTimer timer,
                                                  InsertMessage msg) {

        fdbCfg.run(timer, store -> {

            store.saveRecord(msg.messageState);
            store.saveRecord(msg.textSearch);
        });
    }

    @Benchmark
    public void updateSingleRecordChangeMBoxRef(LuceneConfiguration config,
                                                MailQueryBench.Noncovering fdbCfg,
                                                BenchmarkTimer timer ) {
        fdbCfg.run(timer,store->{

            int nextRecId = timer.getRandom().nextInt(config.numRecords);
            int nextUid = -1 * nextRecId;
            TestMailProto.MessageState.Builder msgStateBuilder = TestMailProto.MessageState.newBuilder();
            TestMailProto.TextSearch.Builder txtSearchBuilder = TestMailProto.TextSearch.newBuilder();
            Message msgState = store.loadRecord(Tuple.from(nextUid)).getRecord();
            if (msgState instanceof DynamicMessage) {
                msgState.getAllFields().forEach((field, value) -> {
                    msgStateBuilder.setField(TestMailProto.MessageState.getDescriptor().findFieldByName(field.getName()), value);
                });
            }
            long newMboxRef = msgStateBuilder.getMBoxRef() + 1;
            //set a new MBoxRef on both
            msgStateBuilder.setMBoxRef(newMboxRef);
            store.saveRecord(msgStateBuilder.build());
        });
    }

    @Benchmark
    public void updateSingleRecordChangeMBoxRefCovered(LuceneConfiguration config,
                                                MailQueryBench.Covering fdbCfg,
                                                BenchmarkTimer timer ) {
        fdbCfg.run(timer,store->{

            int nextRecId = timer.getRandom().nextInt(config.numRecords);
            int nextUid = -1 * nextRecId;
            TestMailProto.MessageState.Builder msgStateBuilder = TestMailProto.MessageState.newBuilder();
            TestMailProto.TextSearch.Builder txtSearchBuilder = TestMailProto.TextSearch.newBuilder();
            Message msgState = store.loadRecord(Tuple.from(nextUid)).getRecord();
            if (msgState instanceof DynamicMessage) {
                msgState.getAllFields().forEach((field, value) -> {
                    msgStateBuilder.setField(TestMailProto.MessageState.getDescriptor().findFieldByName(field.getName()), value);
                });
            }
            long newMboxRef = msgStateBuilder.getMBoxRef() + 1;
            //set a new MBoxRef on both
            msgStateBuilder.setMBoxRef(newMboxRef);
            store.saveRecord(msgStateBuilder.build());

            Message tSearch = store.loadRecord(Tuple.from(nextRecId)).getRecord();
            if (tSearch instanceof DynamicMessage) {
                tSearch.getAllFields().forEach((field, value) -> {
                    txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName(field.getName()), value);
                });
            }
            txtSearchBuilder.setMBoxRef(Long.toString(newMboxRef));
            txtSearchBuilder.setMsgStateUid(msgStateBuilder.getUid());
            store.saveRecord(txtSearchBuilder.build());
        });
    }

    @Benchmark
    public void updateSingleRecordChangeMBoxRefUsingForeignKey(LuceneConfiguration config,
                                                       MailQueryBench.Covering fdbCfg,
                                                       BenchmarkTimer timer ) {
        fdbCfg.run(timer,store->{

            int nextRecId = timer.getRandom().nextInt(config.numRecords);
            int nextUid = -1 * nextRecId;
            TestMailProto.MessageState.Builder msgStateBuilder = TestMailProto.MessageState.newBuilder();
            Message msgState = store.loadRecord(Tuple.from(nextUid)).getRecord();
            if (msgState instanceof DynamicMessage) {
                msgState.getAllFields().forEach((field, value) -> {
                    msgStateBuilder.setField(TestMailProto.MessageState.getDescriptor().findFieldByName(field.getName()), value);
                });
            }
            //set a new MBoxRef
            long newMboxRef = msgStateBuilder.getMBoxRef() + 1;
            msgStateBuilder.setMBoxRef(newMboxRef);

            //now, we have to update the lucene index, so we first lookup the textSearch by it's PK,
            //then modify the correct records, and then save them both back
            Message oldTxtSearch = store.loadRecord(Tuple.from(msgStateBuilder.getTextSearchRef())).getRecord();
            TestMailProto.TextSearch.Builder txtSearchBuilder = TestMailProto.TextSearch.newBuilder();
            if (oldTxtSearch instanceof DynamicMessage) {
                oldTxtSearch.getAllFields().forEach((field, value) -> {
                    txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName(field.getName()), value);
                });
            }
            txtSearchBuilder.setMBoxRef(Long.toString(msgStateBuilder.getMBoxRef()));
            txtSearchBuilder.setMsgStateUid(msgStateBuilder.getUid());

            store.saveRecord(txtSearchBuilder.build());
            store.saveRecord(msgStateBuilder.build());
        });
    }

    @Benchmark
    public void insertIntoForeignKeyTable(LuceneConfiguration config,
                                          MailQueryBench.Covering fdbCfg,
                                          BenchmarkTimer timer,
                                          InsertMessage msg){
        fdbCfg.run(timer,store->{
            //mimicking the algorithm by hand, but the real algorithm can be done more efficiently
            //because it'll avoid re-doing index  maintenance checks by saving the same record twice

            //first, we write the textSearch record as is, because it's the child relation
            store.saveRecord(msg.textSearch);

            /*
             * Now this part is weird--when we write the message state, we look up the associated textSearch
             * field by it's primary key (textSearchRef). Then, we modify textSearchRef to hold the mBoxRef
             * and msgStateUid, then we update the textSearch record again.
             */

            //first, look up the textSearch ref
            Message txtSearchRec = store.loadRecord(Tuple.from(msg.messageState.getTextSearchRef())).getRecord();
            Message.Builder txtSearchBuilder = TestMailProto.TextSearch.newBuilder();
            if (txtSearchRec instanceof DynamicMessage) {
                txtSearchRec.getAllFields().forEach((field, value) -> {
                    txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName(field.getName()), value);
                });
            }

            txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName("mBoxRef"),Long.toString(msg.messageState.getMBoxRef()));
            txtSearchBuilder.setField(TestMailProto.TextSearch.getDescriptor().findFieldByName("msgStateUid"),msg.messageState.getUid());

            //now re-save the textSearch record
            store.saveRecord(msg.textSearch);
            //now save the message state record
            store.saveRecord(msg.messageState);

        });
    }

    public static void main(String... args) throws Exception {
        int numMailboxes = 1;

        Options opt = new OptionsBuilder()
                .forks(0)
                .threads(1)
//                .measurementTime(TimeValue.seconds(1))
                .param("numMailboxes", Integer.toString(numMailboxes))
                .addProfiler("org.openjdk.jmh.profile.RecordLayerProfiler","metric=FETCHES")
                .include(MailUpdateBench.class.getSimpleName() + ".*update.*ForeignKey.*")
                .build();

        new Runner(opt).run();
    }
}

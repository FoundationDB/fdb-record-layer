/*
 * MailQueryBench2.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.benchmark.BenchmarkTimer;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LuceneQueryMultiFieldSearchClause;
import com.apple.foundationdb.record.lucene.LuceneQuerySearchClause;
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.lucene.LuceneScanParameters;
import com.apple.foundationdb.record.lucene.LuceneScanQueryParameters;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.search.Sort;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 60)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MailQueryBench {

    @State(Scope.Benchmark)
    public static class Noncovering extends ConfiguredRecordStore {
        @Setup
        public void setup(LuceneConfiguration params) {
            super.setup(params);
            params.mailboxOffset = 100;
            setName("Noncovering");
            setMetaData(MailBenchmarkUtils.recordStoreMetaData());
        }
    }

    @State(Scope.Benchmark)
    public static class Covering extends ConfiguredRecordStore {
        @Setup
        public void setup(LuceneConfiguration params) {
            super.setup(params);
            params.mailboxOffset = 0;
            setName("Covering");
            setMetaData(MailBenchmarkUtils.mboxInLuceneMetaData());
        }
    }

    @State(Scope.Thread)
    public static class Mailbox {
        private long mBoxId;

        private long mBoxOffset;
        @Setup
        public void setup(LuceneConfiguration config){
            this.mBoxOffset = config.mailboxOffset;
        }

        public long getNextMailbox(int numMailboxes) {
            long mailboxId = mBoxId;
            mBoxId = (mBoxId + 1) % numMailboxes;
            return mailboxId + mBoxOffset;
        }
    }

    @Benchmark
    public void queryMsgStateUidViaCoveringIndex(Covering fdbCfg,
                                                 BenchmarkTimer timer,
                                                 LuceneConfiguration params,
                                                 Mailbox mailbox,
                                                 Blackhole bh) {
        /*
            SELECT msgstate.uid
                FROM textSearch
                JOIN msgstate ON msgstate.textSearchRef = textSearch.\"___recordID\"
                WHERE
                    (msgstate.mboxRef = $mbox)
                AND (lucene('searchBody:\"philodendron monstera\"', textSearch.searchBody))

         */
        final String idxName = "fullSearchIndex";
        fdbCfg.run(timer, store -> {
            long mailboxId = mailbox.getNextMailbox(params.numMailboxes);
            final Index textSearchIdx = store.getRecordMetaData().getIndex(idxName);
            final IndexMaintainer fullSearchIndex = store.getIndexMaintainer(textSearchIdx);
            String luceneQueryString = String.format("searchBody:\"%s\" AND mBoxRef: \"%d\"", params.queryTerm, mailboxId);
            LuceneScanParameters scan = new LuceneScanQueryParameters(
                    ScanComparisons.EMPTY,
                    new LuceneQueryMultiFieldSearchClause(luceneQueryString, false),
                    Sort.RELEVANCE,
                    List.of("msgStateUid"),
                    List.of(LuceneIndexExpressions.DocumentFieldType.INT));
            final LuceneScanBounds scanBounds = scan.bind(store, textSearchIdx, EvaluationContext.EMPTY);

            try (RecordCursor<IndexEntry> textCursor = fullSearchIndex.scan(scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                //the map() call here is to mimic projecting out the uid from the index entry response
                long cnt;
                try {
                    cnt = textCursor.map(ie -> ie.getKey().get(0)).getCount().get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                bh.consume(cnt);
            }
        });
    }

    @Benchmark
    public void queryMsgStateUidViaHashJoin(Noncovering fdbCfg,
                                            LuceneConfiguration params,
                                            Mailbox mailbox,
                                            BenchmarkTimer timer,
                                            Blackhole bh) {
        /*
        An experimental benchmark just to determine whether the problem is the NLJ of the query, or not. We
        start by pre-reading the mailbox that we are interested in, and dumping that into a hashtable that we use
        to look up the results against. This approach won't work in the real world because the size of the memory
        that we'd use up is essentially unbounded, but it serves to inform us of the total cost of different operations
        (such as reading data vs. doing lots of range scans)

            SELECT msgstate.uid
                FROM textSearch
                JOIN msgstate ON msgstate.textSearchRef = textSearch.\"___recordID\"
                WHERE
                    (msgstate.mboxRef = $mbox)
                AND (lucene('searchBody:\"philodendron monstera\"', textSearch.searchBody))

         */
        fdbCfg.run(timer, store -> {

            long mailBox = mailbox.getNextMailbox(params.numMailboxes);
            //load the mailbox data into a hashtable
            //the synthetic data doesn't really create duplicate uids for the same textRef, but in theory it's possible
            // within IMAP, so here we mimic that possibility to create the right data
            Map<Long, List<Long>> refIdToUidMap = new HashMap<>();
            TupleRange range = TupleRange.allOf(Tuple.from(mailBox)); //the entire mailbox range
            try (RecordCursor<IndexEntry> uidCursor = store.scanIndex(store.getRecordMetaData().getIndex(MailBenchmarkUtils.msgStateIndex),
                    IndexScanType.BY_VALUE,
                    range,
                    null,
                    ScanProperties.FORWARD_SCAN)) {
                try {
                    uidCursor.forEach(indexEntry -> {
                        long textRef = indexEntry.getKey().getLong(1);
                        long uid = indexEntry.getKey().getLong(2);
                        List<Long> uids = refIdToUidMap.computeIfAbsent(textRef, (textRefId) -> new ArrayList<>());
                        uids.add(uid);
                    }).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            final Index textSearchIdx = store.getRecordMetaData().getIndex("fullSearchIndex");
            final IndexMaintainer fullSearchIndex = store.getIndexMaintainer(textSearchIdx);
            LuceneScanParameters scan = new LuceneScanQueryParameters(
                    ScanComparisons.EMPTY,
                    new LuceneQuerySearchClause("searchBody", "searchBody:\"" + params.queryTerm + "\"", false));
            final LuceneScanBounds scanBounds = scan.bind(store, textSearchIdx, EvaluationContext.EMPTY);

            try (RecordCursor<IndexEntry> textCursor = fullSearchIndex.scan(scanBounds, null, ScanProperties.FORWARD_SCAN)) {
                long cnt = 0;
                try {
                    cnt = textCursor.map(indexEntry -> {
                        final long textRef = indexEntry.getPrimaryKey().getLong(0);
                        return refIdToUidMap.getOrDefault(textRef, Collections.emptyList()).size();
                    }).reduce(0L, Long::sum).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                bh.consume(cnt);
            }
        });
    }

    @Benchmark
    public void queryMsgStateUidViaNestedLoopJoin(Noncovering fdbCfg,
                                                  LuceneConfiguration params,
                                                  Mailbox mailbox,
                                                  BenchmarkTimer timer,
                                                  Blackhole bh) {
        /*
            SELECT msgstate.uid
                FROM textSearch
                JOIN msgstate ON msgstate.textSearchRef = textSearch.\"___recordID\"
                WHERE
                    (msgstate.mboxRef = $mbox)
                AND (lucene('searchBody:\"philodendron monstera\"', textSearch.searchBody))
         */
        fdbCfg.run(timer, store -> {
            final Index textSearchIdx = store.getRecordMetaData().getIndex("fullSearchIndex");
            final IndexMaintainer fullSearchIndex = store.getIndexMaintainer(textSearchIdx);
            LuceneScanParameters scan = new LuceneScanQueryParameters(
                    ScanComparisons.EMPTY,
                    new LuceneQuerySearchClause("searchBody", "searchBody:\"" + params.queryTerm + "\"", false));
            final LuceneScanBounds scanBounds = scan.bind(store, textSearchIdx, EvaluationContext.EMPTY);

            long mailboxId = mailbox.mBoxId;
            mailbox.mBoxId = (mailbox.mBoxId + 1) % params.numMailboxes;
            try (RecordCursor<IndexEntry> m = RecordCursor.flatMapPipelined(
                    continuation -> fullSearchIndex.scan(scanBounds, continuation, ScanProperties.FORWARD_SCAN),
                    (indexEntry, cont) -> {
                        Tuple textSearchRecId = indexEntry.getPrimaryKey();
                        Tuple key = Tuple.from(mailboxId) //mBoxRef
                                .addAll(textSearchRecId) //textSearchRef == textSearch.___recordID
                                ;
                        TupleRange range = TupleRange.allOf(key);
                        return store.scanIndex(store.getRecordMetaData().getIndex(MailBenchmarkUtils.msgStateIndex),
                                IndexScanType.BY_VALUE,
                                range,
                                cont,
                                ScanProperties.FORWARD_SCAN);
                    },
                    null,
                    2)) {
                long cnt;
                try {
                    cnt = m.map(ie -> ie.getKey().get(2)).getCount().get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                bh.consume(cnt);
            }
        });
    }

    public static void main(String... args) throws Exception {
        int numMailboxes = 1;

        Options opt = new OptionsBuilder()
                .forks(0)
                .threads(1)
                .measurementTime(TimeValue.seconds(1))
                .param("numMailboxes", Integer.toString(numMailboxes))
                .addProfiler("org.openjdk.jmh.profile.RecordLayerProfiler","metric=FETCHES")
                .include(MailQueryBench.class.getSimpleName()+".*CoveringIndex")
                .build();

        new Runner(opt).run();
    }
}

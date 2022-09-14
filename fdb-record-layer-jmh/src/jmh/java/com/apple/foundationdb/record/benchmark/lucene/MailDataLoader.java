/*
 * MailDataLoader.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestMailProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.benchmark.BenchmarkRecordStore;
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import org.apache.commons.math3.util.Pair;

import javax.annotation.Nonnull;
import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Data loader that reads test data from a directory and writes it to the javax.mail structure.
 */
public class MailDataLoader {

    private final RecordMetaData metaData;
    private final MboxDistribution distribution;
    private final int batchSize;

    private final String mailDir;

    public MailDataLoader(@Nonnull RecordMetaData metaData,final MboxDistribution distribution, final int batchSize, final String mailDir) {
        this.metaData = metaData;
        this.distribution = distribution;
        this.batchSize = batchSize;
        this.mailDir = mailDir;
    }

    public void loadData(@Nonnull final FDBDatabase fdbDb,
                         @Nonnull final KeySpacePath ksPath) throws Exception {
        Session session = Session.getDefaultInstance(System.getProperties(), null);

        try (final Stream<Path> fileStream = Files.list(Path.of(mailDir))) {
            final List<Pair<TestMailProto.MessageState, TestMailProto.TextSearch>> loadedData = fileStream.map(path -> generateNext(session, path.getFileName().toString())).collect(Collectors.toList());

            //TODO(bfines) multithreading this might make sense
            List<Pair<TestMailProto.MessageState, TestMailProto.TextSearch>> batch = new ArrayList<>(batchSize);
            for (var elem : loadedData) {
                batch.add(elem);
                if ((batch.size() % batchSize) == 0) {
                    insertBatch(fdbDb, ksPath, batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                insertBatch(fdbDb, ksPath, batch);
            }
        }
    }

    public static void main(String... args) throws Exception {
        String mailDataDir = System.getProperty("user.dir") + "/fdb-record-layer-lucene/src/test/resources/mail_test_data";
        RecordMetaData schemaSetup = MailBenchmarkUtils.mboxInLuceneMetaData();

        FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        KeySpacePath ksPath = BenchmarkRecordStore.KEY_SPACE.path("record-layer-benchmark").add("data").add("name","Covering");

        System.out.println("Loading data");
        long startTime = System.currentTimeMillis();
        new MailDataLoader(schemaSetup,new UniformMbox(1, new Random(0),0), 128, mailDataDir).loadData(fdbDb, ksPath);
        long endTime = System.currentTimeMillis();
        System.out.printf("Data loaded in %s%n", Duration.ofMillis(endTime - startTime));

        try (FDBRecordContext ctx = fdbDb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setFormatVersion(8)
                    .setContext(ctx)
                    .setSerializer(DynamicMessageRecordSerializer.instance())
                    .setKeySpacePath(ksPath)
                    .setMetaDataProvider(schemaSetup)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);

            long recordCount = store.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().get();
            System.out.printf("%d records in store%n", recordCount);

            long idxCount = store.scanIndex(schemaSetup.getIndex("msgStateIdx"),
                    IndexScanType.BY_VALUE,
                    TupleRange.ALL,
                    null,
                    ScanProperties.FORWARD_SCAN).getCount().get();
            System.out.printf("%d records in msgState index%n", idxCount);
        }
    }

    private void insertBatch(FDBDatabase fdbDb, KeySpacePath ksPath, List<Pair<TestMailProto.MessageState, TestMailProto.TextSearch>> batch) {
        try (FDBRecordContext ctx = fdbDb.openContext()) {
            FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setFormatVersion(8)
                    .setContext(ctx)
                    .setSerializer(DynamicMessageRecordSerializer.instance())
                    .setKeySpacePath(ksPath)
                    .setMetaDataProvider(metaData)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);

            for (Pair<TestMailProto.MessageState, TestMailProto.TextSearch> m : batch) {
                store.saveRecord(m.getFirst());
                store.saveRecord(m.getSecond());
            }

            ctx.commit();
        }
    }

    private Pair<TestMailProto.MessageState, TestMailProto.TextSearch> generateNext(Session mailSession, String fileName) {
        File emailFile = new File(mailDir, fileName);
        long emailId = Long.parseLong(fileName.split("\\.")[0]);

        final TestMailProto.MessageState.Builder msgStateBuilder = TestMailProto.MessageState.newBuilder();

        msgStateBuilder.setMBoxRef(distribution.getMailBoxNumber());
        msgStateBuilder.setUid(-1*emailId);

        TestMailProto.TextSearch.Builder txtSearch = readEmail(mailSession, emailFile, emailId);
        msgStateBuilder.setTextSearchRef(txtSearch.getRecordID());
        txtSearch.setMBoxRef(Long.toString(msgStateBuilder.getMBoxRef())).setMsgStateUid(msgStateBuilder.getUid());
        return new Pair<>(msgStateBuilder.build(), txtSearch.build());
    }

    private TestMailProto.TextSearch.Builder readEmail(final Session mailSession, final File emailFile, final long emailId) {
        final TestMailProto.TextSearch.Builder txtSearch = TestMailProto.TextSearch.newBuilder();
        try (InputStream is = new FileInputStream(emailFile)) {
            MimeMessage mm = new MimeMessage(mailSession, is);
            Map<String, String> header = new HashMap<>();
            header.put("bcc", mm.getHeader("Bcc")[0]);
            header.put("to", mm.getHeader("Bcc")[0]);
            header.put("from", mm.getHeader("To")[0]);
            header.put("subject", mm.getSubject());

            final Enumeration<Header> allHeaders = mm.getAllHeaders();
            while (allHeaders.hasMoreElements()) {
                Header nHeader = allHeaders.nextElement();
                txtSearch.addHeader(TestMailProto.StringStringMapEntry.newBuilder()
                        .setKey(nHeader.getName())
                        .setValue(nHeader.getValue())
                        .build());
            }
            txtSearch.addHeader(TestMailProto.StringStringMapEntry.newBuilder().setKey("bcc")
                    .setValue(mm.getHeader("Bcc")[0]).build());
            txtSearch.addHeader(TestMailProto.StringStringMapEntry.newBuilder().setKey("to")
                    .setValue(mm.getHeader("To")[0]).build());
            txtSearch.addHeader(TestMailProto.StringStringMapEntry.newBuilder().setKey("from")
                    .setValue(mm.getHeader("From")[0]).build());
            txtSearch.addHeader(TestMailProto.StringStringMapEntry.newBuilder().setKey("subject")
                    .setValue(mm.getSubject()).build());

            txtSearch.setRecordID(emailId);

            txtSearch.setHeaderBlob(header.toString());
            final Object content = mm.getContent();
            if (content instanceof MimeMultipart) {
                MimeMultipart mmp = (MimeMultipart)content;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < mmp.getCount(); i++) {
                    final BodyPart bodyPart = mmp.getBodyPart(i);
                    if (bodyPart.getContentType().contains("text/plain")) {
                        if (sb.length() > 0) {
                            sb.append(" ");
                        }
                        sb.append(bodyPart.getContent().toString());
                    }
                }
                txtSearch.setSearchBody(sb.toString());
            } else {
                txtSearch.setSearchBody(content.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return txtSearch;
    }

    interface MboxDistribution {

        int getMailBoxNumber();

        long getUid();
    }

    private static class UniformMbox implements MboxDistribution {
        private final int numMailboxes;

        private final Random random;

        private final int mailboxOffset;

        public UniformMbox(final int numMailboxes, final Random random,int mailboxOffset) {
            this.random = random;
            this.numMailboxes = numMailboxes;
            this.mailboxOffset = mailboxOffset;
        }

        @Override
        public int getMailBoxNumber() {
            return random.nextInt(numMailboxes)+mailboxOffset;
        }


        @Override
        public long getUid() {
            return random.nextLong();
        }
    }
}

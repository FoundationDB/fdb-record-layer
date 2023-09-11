/*
 * LuceneMailExampleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;

/**
 *
 * Reference implementation using a generic email test case to be able to scale test
 * the FDB Lucene implementation.
 *
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class LuceneMailExampleTest extends FDBRecordStoreTestBase {
    private static final Logger LOGGER = LogManager.getLogger(LuceneMailExampleTest.class);

    // If this is true, messages are distributed to MAIL_BOX_COUNT mailboxes randomly based on Gaussian distribution
    // If this is false, messages are distributed to the mailboxes according to the MAIL_BOX_DISTRIBUTION
    private static final boolean USE_GAUSSIAN_DISTRIBUTION = false;

    // Use this if mailbox distribution is randomized based on Gaussian distribution
    private static final int MAIL_BOX_COUNT = 10;

    // Change this variable to increase the batch size
    private static final int RECORD_COUNT_BATCH_SIZE = 10;

    // Number of batches to load prior to operation
    private static final int RECORD_COUNT_NUM_BATCHES = 1;

    private static final String DATA_RESOURCE_DIRECTORY = ".out/resources/mail_test_data";

    // Use this to specify the distribution, the number for each index is the sum of all mailboxes' volumes from beginning to it
    private static final int[] MAIL_BOX_DISTRIBUTION = new int[] {1, 11, 111, 611, 1611, 4611, 10000};

    private static final Index MAIL_EXAMPLE_INDEX = new Index("email_lucene", concat(
            function(LuceneFunctionNames.LUCENE_STORED, field("mailbox")),
            field("header", KeyExpression.FanType.FanOut).nest(function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                            concat(
                                    function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
                                    field("key")))),
            function(LuceneFunctionNames.LUCENE_STORED, field("flags")),
            function(LuceneFunctionNames.LUCENE_TEXT, concat(field("body"),
                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())),
                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())))),
            field("isSeen"),
            field("isDeleted"),
            field("isExpunged"),
            field("date"),
            function(LuceneFunctionNames.LUCENE_TEXT, concat(field("headerAsBlob"),
                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name())),
                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS_AND_FREQS_AND_POSITIONS.name()))))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());


    @Test
    public void testReadPerformance() throws Exception {
        loadData();
        LOGGER.info("Query Execution");
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, "Email", MAIL_EXAMPLE_INDEX);
            RecordCursor<IndexEntry> cursor = recordStore.scanIndex(MAIL_EXAMPLE_INDEX, LuceneIndexTestUtils.fullTextSearch(recordStore, MAIL_EXAMPLE_INDEX, "session", false), null, ScanProperties.FORWARD_SCAN);
            Iterator<IndexEntry> entries = cursor.asIterator();
            while (entries.hasNext()) {
                entries.next().getKey();
            }
            commit(context);
        }

    }


    @Test
    public void testWritePerformance() throws Exception {
        loadData();
        LOGGER.info("Write Execution");
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, "Email", MAIL_EXAMPLE_INDEX);
            loadRecords(DATA_RESOURCE_DIRECTORY, 1, 1000);
            commit(context);
        }
    }

    @Test
    public void testUpdatePerformance() throws Exception {
        loadData();
        LOGGER.info("Update Execution");
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, "Email", MAIL_EXAMPLE_INDEX);
            loadRecords(DATA_RESOURCE_DIRECTORY, 1, 1);
            commit(context);
        }
    }

    private void loadData() throws Exception {
        for (int i = 0; i < RECORD_COUNT_NUM_BATCHES; i++) {
            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, "Email", MAIL_EXAMPLE_INDEX);
                loadRecords(DATA_RESOURCE_DIRECTORY, RECORD_COUNT_BATCH_SIZE, i * RECORD_COUNT_BATCH_SIZE);
                commit(context);
            }
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useRewritePlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    /**
     * Load the records for emails from the EML files.
     * @param dataResourceDirectory the directory name to load EML fields from
     * @param recordCountToLoad count of records to load
     * @throws Exception exception
     */
    void loadRecords(String dataResourceDirectory, int recordCountToLoad, int offset) throws Exception {
        final File file = new File(dataResourceDirectory);
        final Properties props = System.getProperties();
        int index = 0;
        int mailBoxIndex = 0; // this is ignored if Gaussian distribution is used
        Random random = new Random();

        long totalWriteTime = 0L;
        // Write mbox records

        List<String> mboxIds = new ArrayList<>();
        for (int i = 1; i <= (USE_GAUSSIAN_DISTRIBUTION ? MAIL_BOX_COUNT : MAIL_BOX_DISTRIBUTION.length); i++) {
            mboxIds.add("box" + i);
        }
        for (File emlFile : file.listFiles()) {
            int mboxId = USE_GAUSSIAN_DISTRIBUTION ? (int)(random.nextGaussian() * (MAIL_BOX_COUNT / 6) + (MAIL_BOX_COUNT / 2 + 1)) : mailBoxIndex + 1;
            mboxId = Math.min(mboxId, MAIL_BOX_COUNT);
            mboxId = Math.max(mboxId, 1);
            Session session = Session.getDefaultInstance(props, null);
            InputStream source = new FileInputStream(emlFile);
            MimeMessage message = new MimeMessage(session, source);
            long start = System.currentTimeMillis();
            try {
                if (loadRecord(message, mboxIds.get(mboxId - 1), index + offset)) {
                    index++;
                }
                if (index >= MAIL_BOX_DISTRIBUTION[mailBoxIndex]) {
                    mailBoxIndex++;
                }
                if (index >= recordCountToLoad) {
                    break;
                }
            } finally {
                totalWriteTime += (System.currentTimeMillis() - start);
                if (index % 100 == 0) {
                    start = System.currentTimeMillis();
                    //zone.flushWithoutReset();
                    totalWriteTime += (System.currentTimeMillis() - start);
                    LOGGER.info("Number of records that have been loaded: {}. Time taken: {} ", index, Duration.ofMillis(totalWriteTime));
                }
            }
        }
        LOGGER.info("Number of records that have been loaded: {}. Time taken: {} ", index, Duration.ofMillis(totalWriteTime));
    }
    
    boolean loadRecord(MimeMessage message, String mboxRecId, long docId) throws Exception {
        TestRecordsTextProto.Email.Builder email = TestRecordsTextProto.Email.newBuilder();
        email.addHeader(TestRecordsTextProto.Email.MailHeader.newBuilder().setKey("bcc").setValue(message.getHeader("Bcc")[0]));
        email.addHeader(TestRecordsTextProto.Email.MailHeader.newBuilder().setKey("to").setValue(message.getHeader("To")[0]));
        email.addHeader(TestRecordsTextProto.Email.MailHeader.newBuilder().setKey("from").setValue(message.getHeader("From")[0]));
        email.addHeader(TestRecordsTextProto.Email.MailHeader.newBuilder().setKey("subject").setValue(message.getSubject()));
        email.setDate(System.currentTimeMillis());
        email.setDocId(docId);
        email.setFlags("xyz");
        email.setIsDeleted(false);
        email.setIsSeen(false);
        email.setIsExpunged(false);
        email.setMailbox(mboxRecId);
        email.setSequence(docId);
        email.setHeaderAsBlob(new StringBuilder()
                .append("bcc ").append(message.getHeader("Bcc")[0])
                .append("to ").append(message.getHeader("To")[0])
                .append("from ").append(message.getHeader("From")[0])
                .append("subject ").append(message.getSubject())
                .toString());

        boolean loaded = false;
        if (message.getContentType().contains("multipart/mixed")) {
            MimeMultipart multipart = (MimeMultipart) message.getContent();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < multipart.getCount(); i++) {
                if (multipart.getBodyPart(i).getContentType().contains("text/plain")) {
                    if (builder.length() > 0) {
                        builder.append(" ");
                    }
                    builder.append(multipart.getBodyPart(i).getContent().toString());
                    loaded = true;
                }
            }
            email.setBody(builder.toString());
            if (loaded) {
                recordStore.saveRecord(email.build());
            }
        }
        return loaded;
    }

}

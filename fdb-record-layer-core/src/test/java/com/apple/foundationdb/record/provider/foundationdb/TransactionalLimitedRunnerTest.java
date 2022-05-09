package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(Tags.RequiresFDB)
class TransactionalLimitedRunnerTest extends FDBTestBase {

    private FDBDatabase fdb;
    private Tuple prefix;

    @BeforeEach
    public void runBefore() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        prefix = Tuple.from(UUID.randomUUID());
    }

    @Test
    void basicTest() {
        // the size of the value is small enough to be inserted, but large enough that it quickly exceeds max transaction
        // size (~100 key/value pairs)
        byte[] value = new byte[100_000];
        try (TransactionalLimitedRunner runner = new TransactionalLimitedRunner(fdb, FDBRecordContextConfig.newBuilder(), 500)
                .setIncreaseLimitAfter(4)
                .setDecreaseLimitAfter(2)) {
            runner.runAsync((context, limit) ->
                    context.ensureActive().getRange(prefix.range(), 1, true)
                            .asList().thenApply(keyValues -> {
                                long starting = 0;
                                if (!keyValues.isEmpty()) {
                                    starting = ((long)Tuple.fromBytes(keyValues.get(0).getKey()).get(1)) + 1;
                                }
                                long l = Math.min(limit, 1_000 - starting);
                                for (int i = 0; i < l; i++) {
                                    context.ensureActive().set(prefix.add(starting + i).pack(), value);
                                }
                                System.out.println(starting + " " + limit);
                                return starting + l < 1_000;
                            }), List.of("loggingKey", "aConstantValue")).join();
        }
        final List<Long> resultingKeys = new AutoContinuingCursor<>(fdb.newRunner(),
                (context, continuation) ->
                        KeyValueCursor.Builder.withSubspace(new Subspace(prefix))
                                .setContext(context)
                                .setScanProperties(ScanProperties.FORWARD_SCAN)
                                .setContinuation(continuation).build()
                                .map(keyValue -> ((long)Tuple.fromBytes(keyValue.getKey()).get(1)))).asList().join();
        assertEquals(LongStream.range(0, 1000).boxed().collect(Collectors.toList()),
                resultingKeys);
    }

    @Test
    void weakReadSemantics() {
        // TODO if config says to use weak read semantics, retries should use new version
    }

    @Test
    void postCommitHookUsage() {
        // TODO add a test where the runnable uses a postCommit hook to do something only for success
    }

    @Test
    void eachAttemptUsesNewTransaction() {
        // TODO make sure it's creating a fresh transaction for every attempt
    }

    @Test
    void prepBeforeTransaction() {
        // TODO test of doing something before the transaction starts
        // The primary reason for having a prep method before the transaction starts is to load some sort of buffer
        // without affecting risking hitting the 5 second timeout.
        // To avoid writing a test that depends on that, which would be both slow, and brittle, this test opens another
        // transaction in prep, and does something that would conflict if the other transaction had already been opened
        // TODO I think the `prep` method itself is unnecessary to achieve this because openContext doesn't do anything,
        //      but the prep would have to be done before the first action, or anything that results in getReadVersion,
        //      which is what starts the clock
    }
}

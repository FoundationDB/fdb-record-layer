package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Tags.RequiresFDB)
class TransactionalLimitedRunnerTest {

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

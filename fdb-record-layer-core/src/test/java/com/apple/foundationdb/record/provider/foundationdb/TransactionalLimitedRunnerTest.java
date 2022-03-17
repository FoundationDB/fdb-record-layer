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
}

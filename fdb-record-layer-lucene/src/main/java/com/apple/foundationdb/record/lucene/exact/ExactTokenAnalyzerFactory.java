package com.apple.foundationdb.record.lucene.exact;

import com.apple.foundationdb.record.lucene.AnalyzerChooser;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerFactory;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerType;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.metadata.Index;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Constructs a new instance of {@link ExactTokenAnalyzer}.
 */
@AutoService(LuceneAnalyzerFactory.class)
public class ExactTokenAnalyzerFactory implements LuceneAnalyzerFactory  {

    public static final String NAME = "QUERY_ONLY_EXACT_ANALYZER_10";

    public static final String UNIQUE_NAME = "query_only_exact_analyzer";


    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public LuceneAnalyzerType getType() {
        return LuceneAnalyzerType.FULL_TEXT;
    }

    @Nonnull
    @Override
    public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index ignored) {
        return new AnalyzerChooser() {
            @Nonnull
            @Override
            public LuceneAnalyzerWrapper chooseAnalyzer(@Nonnull List<String> ignored) {
                return new LuceneAnalyzerWrapper(UNIQUE_NAME, new ExactTokenAnalyzer());
            }
        };
    }

    @Nonnull
    @Override
    public AnalyzerChooser getQueryAnalyzerChooser(@Nonnull Index ignored, @Nonnull AnalyzerChooser alsoIgnored) {
        return new AnalyzerChooser() {
            @Nonnull
            @Override
            public LuceneAnalyzerWrapper chooseAnalyzer(@Nonnull List<String> ignored) {
                return new LuceneAnalyzerWrapper(UNIQUE_NAME, new ExactTokenAnalyzer());
            }
        };
    }
}



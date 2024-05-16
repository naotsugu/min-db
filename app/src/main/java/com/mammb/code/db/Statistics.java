package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;

public class Statistics {

    public record Stat(int numBlocks, int numRecs) {
        public int distinctValues(FieldName fieldName) {
            return 1 + (numRecs / 3);
        }
    }
}

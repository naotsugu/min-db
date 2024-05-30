package com.mammb.code.db.index;

import com.mammb.code.db.lang.DataBox;

public record DirEntry(DataBox<?> dataVal, int blockNumber) {
}

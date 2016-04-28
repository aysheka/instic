package com.fnklabs.instic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class RowData implements Serializable {
    private static final long serialVersionUID = 0L;

    private final String table;
    private final Map<String, byte[]> columnsValue = new HashMap<>();

    public RowData() {
        table = null;
    }

    RowData(String table) {
        this.table = table;
    }

    String getTable() {
        return table;
    }

    void addValue(String column, byte[] value) {
        columnsValue.put(column, value);
    }

    byte[] getValue(String column) {
        return columnsValue.get(column);
    }
}

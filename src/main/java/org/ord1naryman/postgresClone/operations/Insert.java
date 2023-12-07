package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Insert {

    private Insert() {
    }

    public static InsertInto into(Table table) {
        return new InsertInto(table);
    }

    public static class InsertInto {
        private final Table table;
        private InsertInto(Table table) {
            this.table = table;
        }

        /**
         *
         * @param values - map to insert to table, all values that doesn't
         *              exist in table would be ignored
         */
        public InsertInto value(Map<String, Object> values) {
            Map<String, Object> toInsert = new HashMap<>();
            for (var entry : table.getStructure().entrySet()) {
                if (!values.containsKey(entry.getKey())) {
                    throw new IllegalArgumentException("passed map isn't consistent with table");
                }
                Object value = values.get(entry.getKey());
                Class<?> expectedType = table.getStructure().get(entry.getKey());
                if (!value.getClass().isAssignableFrom(expectedType)) {
                    throw new IllegalArgumentException("passed key-value pair must be the same as table's content");
                }
                toInsert.put(entry.getKey(), values.get(entry.getKey()));
            }
            try {
                var tableOOS = table.getObjectOutputStream();
                tableOOS.writeObject(toInsert);
                tableOOS.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }
    }
}

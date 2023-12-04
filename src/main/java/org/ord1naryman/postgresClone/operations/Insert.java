package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Insert {

    public static InsertInto into(Table table) {
        return new InsertInto(table);
    }

    public static class InsertInto {
        private Table table;
        private InsertInto(Table table) {
            this.table = table;
        }
        public InsertInto value(Object value) {
            if (!value.getClass().equals(table.getContainedType())) {
                throw new IllegalArgumentException("value must be the same class " +
                    "as table's content");
            }
            try {
                table.objectOutputStream.writeObject(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }
    }
}

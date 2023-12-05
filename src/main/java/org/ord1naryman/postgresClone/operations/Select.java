package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Select {

    private Select() {
    }

    public static SelectFrom from(Table<?> table) {
        return new SelectFrom(table);
    }

    public static class SelectFrom {
        private final Table<?> table;
        private final Map<String, Object> whereConditions;
        private Comparator<? super Object> activeComparator;
        private SelectFrom(Table<?> table) {
            this.table = table;
            whereConditions = new HashMap<>();
        }

        public <T> SelectFrom where(String fieldName, T value) {
            try {
                Field field = table.getContainedType().getDeclaredField(fieldName);
                if (!field.getType().equals(value.getClass())) {
                    throw new RuntimeException("value type must be the same as field type");
                }
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            whereConditions.put(fieldName, value);
            return this;
        }

        public List<Object> execute() {
            List<Object> returnList = new ArrayList<>();
            try {
                while (true) {
                    Object object = table.objectInputStream.readObject();
                    if (isValidObject(object)) {
                        returnList.add(object);
                    }
                }
            } catch (EOFException e) {
                //objectInputStream doesn't have EOF flag :(
                return returnList;
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public <T> List<T> orderUsingAndExecute(Comparator<T> comparator) {
            try {
                return execute().stream().map(o -> (T) o).sorted(comparator).toList();
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("comparator must compare the same type as table");
            }
        }

        private boolean isValidObject(Object o) {
            try {
                for (var entry : whereConditions.entrySet()) {
                    Field field = o.getClass().getDeclaredField(entry.getKey());
                    field.setAccessible(true);
                    if (!field.get(o).equals(entry.getValue())) {
                        return false;
                    }
                }
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("table in database is inconsistent");
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}

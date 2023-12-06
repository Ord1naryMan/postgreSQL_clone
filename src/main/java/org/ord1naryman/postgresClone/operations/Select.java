package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Select {

    private Select() {
    }

    public static SelectFrom from(Table table) {
        return new SelectFrom(table);
    }

    public static class SelectFrom {
        private final Table table;
        private final Map<String, Object> whereConditions;
        private Comparator<? super Object> activeComparator;
        private List<SelectFrom> unionQueue;
        private SelectFrom(Table table) {
            this.table = table;
            whereConditions = new HashMap<>();
            unionQueue = new ArrayList<>();
        }

        public SelectFrom where(String fieldName, Object value) {
            if (!table.getStructure().get(fieldName).isAssignableFrom(value.getClass())) {
                throw new RuntimeException("value type must be the same as field type");
            }
            whereConditions.put(fieldName, value);
            return this;
        }

        public List<Map<String, Object>> execute() {
            List<Map<String, Object>> returnList = new ArrayList<>();
            try {
                var tableOIS = table.getObjectInputStream();
                tableOIS.readObject(); //skip table type info
                while (true) {
                    Map<String, Object> object = (Map<String, Object>) tableOIS.readObject();
                    if (isValidStructure(object)) {
                        returnList.add(object);
                    }
                }
            } catch (EOFException e) {
                //objectInputStream doesn't have EOF flag :(
                if (!unionQueue.isEmpty()) {
                    return executeUnions(returnList);
                }
                return returnList;
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public SelectFrom union(SelectFrom selectFrom) {
            if (!selectFrom.table.getStructure().equals(table.getStructure())) {
                throw new IllegalArgumentException("union must be used on selections with same output type");
            }
            unionQueue.add(selectFrom);
            return this;
        }

        private boolean isValidStructure(Map<String, Object> structure) {
            for (var entry : whereConditions.entrySet()) {
                if (!entry.getValue().equals(structure.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }

        private List<Map<String, Object>> executeUnions(List<Map<String, Object>> currentList) {
            List<Map<String, Object>> list = new ArrayList<>(currentList);
            for (var select : unionQueue) {
                list.addAll(select.execute());
            }
            return list;
        }
    }
}

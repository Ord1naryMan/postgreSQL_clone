package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SelectFrom {
    private final Table table;
    private final Map<String, Object> whereConditions;
    private List<SelectFrom> unionQueue;
    private String fieldToGroupBy;
    private String orderByField;

    SelectFrom(Table table) {
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

    /**
     * @param fieldName - sets the groupBy field,
     *                  every call to this function rewrites field to groupBy
     */
    public SelectFrom groupBy(String fieldName) {
        fieldToGroupBy = fieldName;
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
                returnList = executeUnions(returnList);
            }
            if (fieldToGroupBy != null &&
                table.getStructure().containsKey(fieldToGroupBy)) {
                returnList = executeGrouping(returnList, fieldToGroupBy);
            }
            if (orderByField != null &&
                table.getStructure().containsKey(orderByField)) {
                return executeOrdering(returnList, orderByField);
            }
            return returnList;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Map<String, Object>> executeOrdering(List<Map<String, Object>> returnList, String orderByField) {
        returnList.sort((o1, o2) ->
            ((Comparable)o1.get(orderByField)).compareTo(o2.get(orderByField))
        );
        return returnList;
    }

    private List<Map<String, Object>> executeGrouping(List<Map<String, Object>> toReturnAfterUnions, String fieldToGroupBy) {
        List<Map<String, Object>> afterGrouping = new ArrayList<>();
        Set<Object> usedValues = new HashSet<>();
        while (!toReturnAfterUnions.isEmpty()) {
            var valueToCompare = toReturnAfterUnions.get(0).get(fieldToGroupBy);
            usedValues.add(valueToCompare);
            for (var value : toReturnAfterUnions) {
                if (value.get(fieldToGroupBy).equals(valueToCompare)) {
                    afterGrouping.add(value);
                }
            }
            //search for next nonUsed value to groupBy and removing all used values
            while (!toReturnAfterUnions.isEmpty() &&
                usedValues.contains(toReturnAfterUnions.get(0).get(fieldToGroupBy))) {
                toReturnAfterUnions.remove(0);
            }
        }
        return afterGrouping;
    }

    public SelectFrom union(SelectFrom selectFrom) {
        if (!selectFrom.table.getStructure().equals(table.getStructure())) {
            throw new IllegalArgumentException("union must be used on selections with same output type");
        }
        unionQueue.add(selectFrom);
        return this;
    }

    public Join join(SelectFrom selectFrom) {
        return new Join(execute(), selectFrom.execute());
    }

    public SelectFrom orderBy(String fieldName) {
        var fieldClass = table.getStructure().get(fieldName);
        if (fieldClass == null) {
            throw new IllegalArgumentException("cannot sort using fields that doesn't exists");
        }
        if (!Comparable.class.isAssignableFrom(fieldClass)) {
            throw new IllegalArgumentException("provided field's value doesn't implement Comparable");
        }
        orderByField = fieldName;
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
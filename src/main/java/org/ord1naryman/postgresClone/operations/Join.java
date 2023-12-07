package org.ord1naryman.postgresClone.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Join {

    private List<Map<String, Object>> dataLeft;
    private List<Map<String, Object>> dataRight;

    Join(List<Map<String, Object>> dataLeft, List<Map<String, Object>> dataRight) {
        this.dataLeft = dataLeft;
        this.dataRight = dataRight;
    }

    /**
     * @return joint two tables where left fields has higher priority
     */
    public Join on(String leftField, String rightField) {
        if (dataLeft.isEmpty() || dataRight.isEmpty()) {
            throw new IllegalArgumentException("join can't be used on empty tables without field names");
        }
        if (!dataLeft.get(0).containsKey(leftField) || !dataRight.get(0).containsKey(rightField)) {
            throw new IllegalArgumentException("table must contain field with provided name");
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (var itemLeft : dataLeft) {
            for (var itemRight : dataRight) {
                if (itemLeft.get(leftField).equals(itemRight.get(rightField))) {
                    var dataToAdd = new HashMap<String, Object>();
                    dataToAdd.putAll(itemRight);
                    dataToAdd.putAll(itemLeft);
                    result.add(dataToAdd);
                }
            }
        }
        dataLeft = result;
        dataRight = null;
        return this;
    }

    public Join join(SelectFrom selectFrom) {
        if (dataRight != null) {
            throw new IllegalArgumentException("firstly use method 'on' after previous join and then make another join");
        }
        dataRight = selectFrom.execute();
        return this;
    }

    public List<Map<String, Object>> execute() {
        if (dataRight != null) {
            throw new IllegalArgumentException("please run 'on' after 'join'");
        }
        return dataLeft;
    }


}

package org.ord1naryman.postgresClone.operations;

import org.ord1naryman.postgresClone.model.Table;

public class Select {

    private Select() {
    }

    public static SelectFrom from(Table table) {
        return new SelectFrom(table);
    }

}

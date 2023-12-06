package org.ord1naryman.postgresClone.model;

import org.ord1naryman.postgresClone.core.ConnectionPool;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class Database {

    private String name;

    public Database(String name) {
        this.name = name;
    }

    public Table openTable(String name) {
        if (ConnectionPool.openConnections.containsKey(this.name + "." + name)) {
            return ConnectionPool.openConnections
                .get(this.name + "." + name);
        }

        File file = new File("data/" + this.name + "." + name);
        if (!file.exists()) {
            throw new IllegalArgumentException("Current table doesn't exists, please provide table structure");
        }

        var newTable = new Table(this.name, name, file);
        ConnectionPool.openConnections.put(this.name + "." + name, newTable);
        return newTable;

    }

    public Table createTable(String name, Map<String, Class<?>> structure) {
        if (ConnectionPool.openConnections.containsKey(this.name + "." + name)) {
            throw new IllegalArgumentException("table already exists, please use openTable method");
        }
        File file = new File("data/" + this.name + "." + name);

        if (file.exists()) {
            throw new IllegalArgumentException("table already exists, please use openTable method");
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Table newTable = new Table(this.name, name, file, structure);
        ConnectionPool.openConnections.put(this.name + "." + name, newTable);
        return newTable;
    }
}

package org.ord1naryman.postgresClone.model;

import org.ord1naryman.postgresClone.core.ConnectionPool;

import java.io.Serializable;

public class Database {

    private String name;

    public Database(String name) {
        this.name = name;
    }

    public <T> Table<T> createTable(String name, Class<T> clazz) {
        if (!Serializable.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("passed class should implement Serializable");
        }
        if (ConnectionPool.openConnections.containsKey(this.name + "." + name)) {
            Table<?> table = ConnectionPool.openConnections
                .get(this.name + "." + name);
            if (!clazz.equals(table.getContainedType())) {
                throw new IllegalArgumentException("passed class isn't the same as table's class");
            }
            return (Table<T>) table;
        }
        Table<T> newTable = new Table<T>(this.name, name, clazz);
        ConnectionPool.openConnections.put(this.name + "." + name, newTable);
        return newTable;
    }
}

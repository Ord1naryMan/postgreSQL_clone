package org.ord1naryman.postgresClone.model;

import org.ord1naryman.postgresClone.core.ConnectionPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Logger;

public class Table<T> {

    private final String databaseName;
    private String tableName;

    private File file;
    private final Class<T> containedType;
    public ObjectInputStream objectInputStream;
    public ObjectOutputStream objectOutputStream;
    private Logger log = Logger.getLogger("Table");

    Table(String databaseName, String tableName, Class<T> clazz) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        containedType = clazz;

        file = new File("data/" + databaseName + "." + tableName);

        try {
            if (file.exists()) {
                log.warning("table was not created, table with name " +
                    file.getName() + " already exists");
            } else {
                if (!file.createNewFile()) {
                    throw new IllegalArgumentException("file was not created");
                }
            }
            objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
            objectInputStream = new ObjectInputStream(new FileInputStream(file));
            if (file.getTotalSpace() > 0) {
                if (!isFileConsistentWithTable(objectInputStream)) {
                    throw new RuntimeException("table content doesn't match provided class");
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Class<T> getContainedType() {
        return containedType;
    }

    public File getFile() {
        return file;
    }

    private boolean isFileConsistentWithTable(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        if (ois.available() <= 0) {
            return true;
        }
        Object o = ois.readObject();
        return containedType.isAssignableFrom(o.getClass());
    }

    public void close() {
        try {
            objectOutputStream.close();
            objectInputStream.close();
            ConnectionPool.openConnections.remove(databaseName + "." + tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

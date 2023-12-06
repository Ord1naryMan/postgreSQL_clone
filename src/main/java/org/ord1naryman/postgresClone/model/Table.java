package org.ord1naryman.postgresClone.model;

import org.ord1naryman.postgresClone.core.ConnectionPool;
import org.ord1naryman.postgresClone.utlis.AppendingObjectOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Table {

    private final String databaseName;
    private final String tableName;

    private File file;
    private Map<String, Class<?>> structure;

    private AppendingObjectOutputStream objectOutputStream;
    private ObjectInputStream objectInputStream;

    Table(String databaseName, String tableName, File file) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.file = file;


        try {
            objectOutputStream = new AppendingObjectOutputStream(new FileOutputStream(file, true));
            getObjectInputStream();
            structure = (Map<String, Class<?>>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    Table(String databaseName, String tableName, File file, Map<String, Class<?>> structure) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.structure = structure;
        this.file = file;

        try {
            var tempOOS = new ObjectOutputStream(new FileOutputStream(file));
            tempOOS.writeObject(structure);
            tempOOS.close();
            objectOutputStream = new AppendingObjectOutputStream(new FileOutputStream(file, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Class<?>> getStructure() {
        return structure;
    }

    public File getFile() {
        return file;
    }

    public void close() {
        closeStreams();
        ConnectionPool.openConnections.remove(databaseName + "." + tableName);
    }

    public ObjectOutputStream getObjectOutputStream() {
        return objectOutputStream;
    }

    public ObjectInputStream getObjectInputStream() {
        try {
            objectInputStream = new ObjectInputStream(new FileInputStream(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objectInputStream;
    }

    private void closeStreams() {
        try {
            if (objectInputStream != null) {
                objectInputStream.close();
            }
            if (objectOutputStream != null) {
                objectOutputStream.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

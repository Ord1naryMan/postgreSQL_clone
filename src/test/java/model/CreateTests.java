package model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CreateTests {

    Table table;

    Map<String, Class<?>> testStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "name", String.class
        )
    );

    @AfterEach
    void deleteTestTable() {
        table.close();
        table.getFile().delete();
    }

    @BeforeEach
    void createTable() {
        table = new Database("test").createTable("test", testStructure);
    }

    @Test
    void createTableTest() {
        assertTrue(new File("data/test.test").exists());
        assertThrows(IllegalArgumentException.class, () ->
            new Database("test").createTable("test", testStructure)
        );
    }

    @Test
    void openTableWithoutCachingTest() {
        Table t = new Database("test").createTable("test1", testStructure);
        Insert.into(t).value(Map.of("id", 1, "name", "123"));
        t.close();
        try {
            t = new Database("test").openTable("test1");
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            t.close();
            t.getFile().delete();
        }
        assertThrows(IllegalArgumentException.class,
            () -> new Database("test").openTable("non-existent-table"));
    }

    @Test
    void openTableFromCache() {
        var t2 = new Database("test").openTable("test");
        assertEquals(table, t2);
        t2.close();
    }
}
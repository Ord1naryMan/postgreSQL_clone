package model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CreateTests {

    Table<TestData> table;

    @AfterEach
    void deleteTestTable() throws IOException {
        table.close();
        table.getFile().delete();
    }

    @BeforeEach
    void createTable() {
        table = new Database("test").createTable("test", TestData.class);
    }

    @Test
    void createTableTest() {
        assertTrue(new File("data/test.test").exists());
        assertThrows(IllegalArgumentException.class, () ->
            new Database("test").createTable("test1", TestDataNotSerializable.class)
        );
    }

    @Test
    void createTableMultipleTimes() throws IOException {
        var t1 = new Database("test").createTable("test", TestData.class);
        var t2 = new Database("test").createTable("test", TestData.class);
        assertEquals(t1, t2);
        t1.close();
        t2.close();
    }

    @Test
    void openTableWithWrongDataType() {
        //keep in mind table "test.test" has already been created look at (before each)
        assertThrows(IllegalArgumentException.class, () ->
            new Database("test").createTable("test", MoreTestData.class)
        );
    }

    @Test
    void openCachedTableWithWrongContainedDataType() throws IOException {
        Insert.into(table).value(new TestData());
        assertThrows(IllegalArgumentException.class, () ->
            new Database("test").createTable("test", MoreTestData.class)
        );
    }
}

class TestData implements Serializable {
    public Integer id;
    public String name;
}

class TestDataNotSerializable {
    public String name;
}

class MoreTestData implements Serializable {
    public Integer id;
}
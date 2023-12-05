package operations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class InsertTests {

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
    void writeValueWithWrongType() {
        assertThrows(IllegalArgumentException.class, () ->
            Insert.into(table).value(new TestData(1, "test1")).value(new MoreTestData())
        );
    }

    @Test
    void insertValue() throws IOException, ClassNotFoundException {
        var data = new TestData(1, "test");
        Insert.into(table).value(data);
        TestData retrievedData = (TestData) table.objectInputStream.readObject();
        assertEquals(data.id, retrievedData.id);
        assertEquals(data.name, retrievedData.name);
    }
}

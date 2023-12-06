package operations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class InsertTests {

    Table table;

    Map<String, Class<?>> testStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "name", String.class
        )
    );

    @AfterEach
    void deleteTestTable() throws IOException {
        table.close();
        table.getFile().delete();
    }

    @BeforeEach
    void createTable() {
        table = new Database("test").createTable("test", testStructure);
    }
    @Test
    void writeValueWithWrongType() {
        assertThrows(IllegalArgumentException.class, () ->
            Insert.into(table)
                .value(Map.of("id", 1, "name", "123"))
                .value(Map.of("id", 1))
        );
    }

    @Test
    void insertValue() throws IOException, ClassNotFoundException {
        Insert.into(table).value(Map.of("id", 1, "name", "123"));
        var tableOIS = table.getObjectInputStream();
        tableOIS.readObject(); //skip table data type
        Map<String, Object> retrievedData = (Map<String, Object>) tableOIS.readObject();
        assertEquals(1, retrievedData.get("id"));
        assertEquals("123", retrievedData.get("name"));
    }
}

package operations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;
import org.ord1naryman.postgresClone.operations.Select;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SelectTest {

    Table table;

    Map<String, Class<?>> testStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "name", String.class
        )
    );

    Map<String, Class<?>> secondTestStructure = new HashMap<>(
        Map.of(
            "id", Integer.class
        )
    );

    @AfterEach
    void deleteTestTable() {
        table.deleteFile();
    }

    @BeforeEach
    void createTable() {
        table = new Database("test").createTable("test", testStructure);
    }

    @Test
    void selectAllTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<Map<String, Object>> list = Select.from(table).execute();
        assertEquals(genTestData(), list);
    }

    @Test
    void selectWithSingleWhereTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<Map<String, Object>> list = Select.from(table)
            .where("id", 1)
            .execute();
        var expected = List.of(
            Map.of("id", 1, "name", "test1"),
            Map.of("id", 1, "name", "test4")
        );
        assertEquals(expected, list);
    }

    @Test
    void selectWithMultipleWhereTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<Map<String, Object>> list = Select.from(table)
            .where("id", 1)
            .where("name", "test1")
            .execute();
        assertEquals(List.of(Map.of("id", 1, "name", "test1")), list);
    }

    @Test
    void selectWithUnionTest() {
        genTestData().forEach(Insert.into(table)::value);

        var t1 = new Database("test").createTable("test1", testStructure);
        genTestData().forEach(Insert.into(t1)::value);

        List<Map<String, Object>> actual = Select.from(table)
            .union(Select.from(t1))
            .execute();

        assertEquals(8, actual.size());

        var expected = new ArrayList<Map<String, Object>>() {{
            addAll(genTestData());
            addAll(genTestData());
        }};

        assertEquals(expected, actual);

        t1.deleteFile();
    }

    @Test
    void selectWithUnionWrongTypeTest() {
        genTestData().forEach(Insert.into(table)::value);

        var t1 = new Database("test").createTable("test1", secondTestStructure);

        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).union(Select.from(t1)).execute()
        );


        t1.deleteFile();
    }

    List<Map<String, Object>> genTestData() {
        return List.of(
            Map.of("id", 1, "name", "test1"),
            Map.of("id", 2, "name", "test2"),
            Map.of("id", 3, "name", "test3"),
            Map.of("id", 1, "name", "test4")
        );
    }
}

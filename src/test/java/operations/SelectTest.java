package operations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;
import org.ord1naryman.postgresClone.operations.Select;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

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
    void whereBetweenTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<Map<String, Object>> list = Select.from(table)
            .whereBetween("id", 1, 2)
            .execute();

        list.forEach(o -> {
            if ((int) o.get("id") > 2 || (int) o.get("id") < 0) {
                fail();
            }
        });
    }

    @Test
    void whereBetweenPassedValueDoesntImplementComparableTest() {
        var table2 = new Database("test").createTable("test2", Map.of("id", TestClass.class));
        Insert.into(table2).value(Map.of("id", new TestClass(1)));
        Insert.into(table2).value(Map.of("id", new TestClass(2)));
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table2).whereBetween("id", new TestClass(1), new TestClass(2)).execute()
        );
        table2.deleteFile();
    }

    @Test
    void whereBetweenIllegalFieldNameTest() {
        genTestData().forEach(Insert.into(table)::value);
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).whereBetween("FieldThatDoesntExist", 1, 2).execute()
        );
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

    @Test
    void groupByTest() {
        genTestData().forEach(Insert.into(table)::value);
        var actual = Select.from(table).groupBy("id").execute();
        assertEquals(actual.get(0).get("id"), actual.get(1).get("id"));
    }

    @Test
    void groupByOnNonExistentField() {
        genTestData().forEach(Insert.into(table)::value);
        var actual = Select.from(table).groupBy("notExist").execute();
        assertEquals(actual, genTestData());
    }

    @Test
    void persistenceAfterReopeningFileTest() {
        Map<String, Object> testValue = Map.of("id", 1, "name", "123");
        Insert.into(table).value(testValue);
        table.close();
        assertThrows(IllegalArgumentException.class, () ->
            new Database("test").createTable("test", testStructure));
        table = new Database("test").openTable("test");
        var actual = Select.from(table).execute();
        assertEquals(testValue, actual.get(0));
    }

    @Test
    void orderByTest() {
        genTestData().forEach(Insert.into(table)::value);
        var actual = Select.from(table).orderBy("id").execute();
        var expected = genTestData().stream()
            .sorted((o1, o2) ->
                ((Comparable) o1.get("id")).compareTo(o2.get("id"))
            ).toList();
        assertEquals(expected, actual);
    }

    @Test
    void orderByWithInappropriateValueTest() {
        genTestData().forEach(Insert.into(table)::value);
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).orderBy("FIEEEEEEELD").execute()
        );

        var table2 = new Database("test").createTable("test1", Map.of("id", Integer.class, "name", TestClass.class));
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table2).orderBy("name").execute()
        );
        table2.deleteFile();
    }

    @Test
    void sumByTest() {
        genTestData().forEach(Insert.into(table)::value);
        var actual = Select.from(table).sumBy("id");
        assertEquals(7, actual);
    }

    @Test
    void sumByWrongPassedValueTest() {
        genTestData().forEach(Insert.into(table)::value);
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).sumBy("notExist")
        );

        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).sumBy("name")
        );
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

class TestClass implements Serializable {
    int id;

    TestClass(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}

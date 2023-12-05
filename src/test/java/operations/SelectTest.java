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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SelectTest {

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
    void selectAllTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<TestData> list = Select.from(table).execute()
            .stream().map(o -> (TestData) o).toList();
        assertEquals(genTestData(), list);
    }

    @Test
    void selectWithSingleWhereTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<TestData> list = Select.from(table)
            .where("id", 1)
            .execute()
            .stream().map(o -> (TestData) o).toList();
        var expected = List.of(new TestData(1, "test1"),
            new TestData(1, "test4"));
        assertEquals(expected, list);
    }

    @Test
    void selectWithMultipleWhereTest() {
        genTestData().forEach(Insert.into(table)::value);
        List<TestData> list = Select.from(table)
            .where("id", 1)
            .where("name", "test1")
            .execute()
            .stream().map(o -> (TestData) o).toList();
        assertEquals(List.of(new TestData(1, "test1")), list);
    }

    @Test
    void selectWithOrderUsingTest() {
        genTestData().forEach(Insert.into(table)::value);

        var comparator = Comparator.comparingInt((TestData o) -> o.id);

        var actual = Select.from(table).orderUsingAndExecute(comparator);
        var expected = new ArrayList<>(genTestData());
        expected.sort(comparator);
        assertEquals(expected, actual);
    }

    @Test
    void selectWithComparatorOfWrongTypeTest() {
        genTestData().forEach(Insert.into(table)::value);

        var comparator = Comparator.comparingInt((MoreTestData o) -> o.id);
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).orderUsingAndExecute(comparator)
        );
    }

    @Test
    void selectWithUnionTest() {
        genTestData().forEach(Insert.into(table)::value);

        var t1 = new Database("test").createTable("test1", TestData.class);
        genTestData().forEach(Insert.into(t1)::value);

        List<TestData> actual = Select.from(table).union(Select.from(t1))
            .execute().stream().map(o -> (TestData) o).toList();

        assertEquals(8, actual.size());

        var expected = new ArrayList<TestData>() {{
            addAll(genTestData());
            addAll(genTestData());
        }};

        assertEquals(expected, actual);

        t1.close();
        t1.getFile().delete();
    }

    @Test
    void selectWithUnionWrongTypeTest() {
        genTestData().forEach(Insert.into(table)::value);

        var t1 = new Database("test").createTable("test1", MoreTestData.class);

        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table).union(Select.from(t1)).execute()
        );


        t1.close();
        t1.getFile().delete();
    }

    List<TestData> genTestData() {
        return List.of(
            new TestData(1, "test1"),
            new TestData(2, "test2"),
            new TestData(3, "test3"),
            new TestData(1, "test4")
        );
    }
}

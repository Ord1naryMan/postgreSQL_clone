package operations;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;
import org.ord1naryman.postgresClone.operations.Select;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    List<TestData> genTestData() {
        return List.of(
            new TestData(1, "test1"),
            new TestData(2, "test2"),
            new TestData(3, "test3"),
            new TestData(1, "test4")
        );
    }
}

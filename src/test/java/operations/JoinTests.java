package operations;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.model.Table;
import org.ord1naryman.postgresClone.operations.Insert;
import org.ord1naryman.postgresClone.operations.Select;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JoinTests {

    Table table1;
    Table table2;
    Table table3;
    Table table4;

    Map<String, Class<?>> testStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "name", String.class
        )
    );

    Map<String, Class<?>> secondTestStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "second_name", String.class
        )
    );

    Map<String, Class<?>> thirdTestStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "third_name", String.class
        )
    );

    List<Map<String, Object>> firstAndSecond = List.of(
        Map.of("id", 1, "name", "name1", "second_name", "second_name1"),
        Map.of("id", 2, "name", "name2", "second_name", "second_name2"),
        Map.of("id", 3, "name", "name3", "second_name", "second_name3")
    );

    List<Map<String, Object>> firstAndSecondAndThird = List.of(
        Map.of("id", 1, "name", "name1", "second_name", "second_name1", "third_name", "third_name1"),
        Map.of("id", 2, "name", "name2", "second_name", "second_name2", "third_name", "third_name2"),
        Map.of("id", 3, "name", "name3", "second_name", "second_name3", "third_name", "third_name3")
    );

    List<Map<String, Object>> joinWithWhereExpected = List.of(
        Map.of("id", 1, "name", "name1", "second_name", "second_name1")
    );

    @AfterEach
    void deleteTestTable() {
        table1.deleteFile();
        table2.deleteFile();
        table3.deleteFile();
        table4.deleteFile();
    }

    @BeforeEach
    void createTable() {
        var database = new Database("test");
        table1 = database.createTable("test1", testStructure);
        table2 = database.createTable("test2", secondTestStructure);
        table3 = database.createTable("test3", thirdTestStructure);
        table4 = database.createTable("test4", testStructure);
        genTable1TestData().forEach(Insert.into(table1)::value);
        genTable2TestData().forEach(Insert.into(table2)::value);
        genTable3TestData().forEach(Insert.into(table3)::value);
        genTable1ButSlightlyDifferent().forEach(Insert.into(table4)::value);
    }

    @Test
    void normalJoinTest() {
        var actual = Select.from(table1)
            .join(Select.from(table2))
            .on("id", "id")
            .execute();


        assertEquals(firstAndSecond, actual);
    }

    @Test
    void joinWithWhere() {
        var actual1 = Select.from(table1).where("id", 1)
            .join(Select.from(table2))
            .on("id", "id")
            .execute();

        var actual2 = Select.from(table1)
            .join(Select.from(table2).where("id", 1))
            .on("id", "id")
            .execute();

        assertEquals(joinWithWhereExpected, actual1);
        assertEquals(joinWithWhereExpected, actual2);
    }

    @Test
    void priorityInJoinsTest() {
        var actual = Select.from(table4)
            .join(Select.from(table1))
            .on("id", "id")
            .execute();
        assertEquals(actual, genTable1ButSlightlyDifferent());
    }

    @Test
    void twoJoinsTest() {
        var actual = Select.from(table1)
            .join(Select.from(table2))
            .on("id", "id")
            .join(Select.from(table3))
            .on("id", "id")
            .execute();

        assertEquals(firstAndSecondAndThird, actual);
    }

    @Test
    void twoJoinsWithoutOnTest() {
        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table1)
                .join(Select.from(table2))
                .execute()
        );

        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table1)
                .join(Select.from(table2))
                .on("id", "id")
                .join(Select.from(table3))
                .execute()
        );

        assertThrows(IllegalArgumentException.class, () ->
            Select.from(table1)
                .join(Select.from(table2))
                .join(Select.from(table3))
                .on("id", "id")
                .execute()
        );
    }

    List<Map<String, Object>> genTable1TestData() {
        return List.of(
            Map.of("id", 1, "name", "name1"),
            Map.of("id", 2, "name", "name2"),
            Map.of("id", 3, "name", "name3")
        );
    }

    List<Map<String, Object>> genTable2TestData() {
        return List.of(
            Map.of("id", 1, "second_name", "second_name1"),
            Map.of("id", 2, "second_name", "second_name2"),
            Map.of("id", 3, "second_name", "second_name3")
        );
    }

    List<Map<String, Object>> genTable3TestData() {
        return List.of(
            Map.of("id", 1, "third_name", "third_name1"),
            Map.of("id", 2, "third_name", "third_name2"),
            Map.of("id", 3, "third_name", "third_name3")
        );
    }

    List<Map<String, Object>> genTable1ButSlightlyDifferent() {
        return List.of(
            Map.of("id", 1, "name", "slightly_different_name1"),
            Map.of("id", 2, "name", "slightly_different_name12"),
            Map.of("id", 3, "name", "slightly_different_name13")
        );
    }
}

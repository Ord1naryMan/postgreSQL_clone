package operations;

import java.io.Serializable;
import java.util.Objects;

class TestData implements Serializable {
    public Integer id;
    public String name;

    public TestData(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestData testData = (TestData) o;
        return Objects.equals(id, testData.id) && Objects.equals(name, testData.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}

class TestDataNotSerializable{
    public String name;
}

class MoreTestData implements Serializable {
    public Integer id;
}
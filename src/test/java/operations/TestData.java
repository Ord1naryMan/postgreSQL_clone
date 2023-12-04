package operations;

import java.io.Serializable;

class TestData implements Serializable {
    public Integer id;
    public String name;
}

class TestDataNotSerializable{
    public String name;
}

class MoreTestData implements Serializable {
    public Integer id;
}
# PostgreSQL clone
This repo was made in educational purposes.
This is more adaptation than clone.

# Features
SQL commands recreated with use of java classes

- create (database / table)
- insert data into table
- select ('where' is supported)
- order by (more like orderUsing(Comparator))
- union
- join
- group by

# Data storage
Every table is stored in 'data' folder in root directory. 
Each table is named as following 'databaseName.tableName' and data 
is stored in files as serialized java Map<String, Object>.
There is only one concurrent connection to each table, 
that means that concurrency isn't supported, and 
after opening connection equality of stored data type and data type used 
to open connection is checked.

# Test coverage
<div style="color: greenyellow">100% method coverage</div>
<div style="color: yellow">88% line coverage</div>

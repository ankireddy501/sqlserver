# sqlserver
DeleteTables project explans how to drop the tables exists in a schema, by dropping all the forgien key contraints first then dropping tables.

mvn install:install-file -Dfile=sqljdbc4.jar -Dpackaging=jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0

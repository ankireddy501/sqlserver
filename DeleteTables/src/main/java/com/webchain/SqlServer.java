package com.webchain;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anki on 01-09-2016.
 */
public class SqlServer {

    public static void main(String args[]) throws Exception{

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver",false, SqlServer.class.getClassLoader());

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl("jdbc:sqlserver://localhost;databaseName=webchain;instanceName=SQLEXPRESS;");
        dataSource.setUsername("webchain");
        dataSource.setPassword("webchain");
        Connection connection = dataSource.getConnection();
        String schemaName = "webchain";
        setDefaultSchema(schemaName,connection );
        Statement st = connection.createStatement();

        ResultSet tableResultSet = connection.getMetaData().getTables(null, schemaName, "%", new String[] {"TABLE"});

        List<String> tableNameList = new ArrayList<String>();
        while (tableResultSet.next()) {
            String tableName = tableResultSet.getString("TABLE_NAME");
            tableNameList.add(tableName);
        }
        tableResultSet.close();
        removeConstraints(connection, schemaName);
        for (String tableName : tableNameList) {
            String fullyQualifiedTableName = null;
            fullyQualifiedTableName = "\"" + schemaName + "\"" + "." + "\"" + tableName + "\"";
            String dropTableQuery = "DROP TABLE " + fullyQualifiedTableName;
            st.executeUpdate(dropTableQuery);
        }
    }

    private static void removeConstraints(Connection connection, String schemaName) throws SQLException{

            String findConstraints = "SELECT  obj.name AS fk_name,\n" +
                    "    sch.name AS [schema_name],\n" +
                    "    tab1.name AS [table_name],\n" +
                    "    col1.name AS [column],\n" +
                    "    tab2.name AS [referenced_table],\n" +
                    "    col2.name AS [referenced_column]\n" +
                    "FROM sys.foreign_key_columns fkc\n" +
                    "INNER JOIN sys.objects obj\n" +
                    "    ON obj.object_id = fkc.constraint_object_id\n" +
                    "INNER JOIN sys.tables tab1\n" +
                    "    ON tab1.object_id = fkc.parent_object_id\n" +
                    "INNER JOIN sys.schemas sch\n" +
                    "    ON tab1.schema_id = sch.schema_id\n" +
                    "INNER JOIN sys.columns col1\n" +
                    "    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id\n" +
                    "INNER JOIN sys.tables tab2\n" +
                    "    ON tab2.object_id = fkc.referenced_object_id\n" +
                    "INNER JOIN sys.columns col2\n" +
                    "    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id where sch.name=?";

            PreparedStatement preparedStatement = connection.prepareStatement(findConstraints);

            preparedStatement.setString(1, schemaName);
            ResultSet fkResults = preparedStatement.executeQuery();
            while(fkResults.next()){
                String fkName = fkResults.getString("fk_name");
                String tableName = fkResults.getString("table_name");
                Statement dropConstraintStatement = null;
                try{
                    dropConstraintStatement = connection.createStatement();
                    String dropConstraintQuery = "ALTER TABLE "+tableName+" DROP CONSTRAINT  "+fkName;
                    System.out.println("dropConstraintQuery "+dropConstraintQuery);
                    dropConstraintStatement.executeUpdate(dropConstraintQuery);
                }finally{
                    if(dropConstraintStatement != null){
                       dropConstraintStatement.close();
                    }
                }
            }
    }

    /**
     * Set default schema to use.
     *
     * @param connection connection point
     */
    public static void setDefaultSchema(String schema, Connection connection)
    {
        Statement statement = null;
        try
        {
            statement = connection.createStatement();

            // query for default schema set
            List<String> switchToSchema = getSwitchToSchemaSQL(schema);
            for(String sqlStatement : switchToSchema){
                statement.execute(sqlStatement);
            }
            connection.commit();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Cannot set default schema: " + schema, e);
        }
        finally
        {
            // release resources
            closeStatement(statement);
        }
    }

    /**
     * the connection uses the specified user during all operations.
     * @param schemaName schema user name (our assumption is schema users username and schema name both are same)
     * @return list of sql command to switch the schema to specified schema
     */
    public static List<String> getSwitchToSchemaSQL(String schemaName) {
        final List<String> sqls = new ArrayList<String>(2);

        sqls.add("REVERT");
        sqls.add("EXECUTE AS USER = '" + schemaName + "'");

        return sqls;
    }

    /**
     * Close the given statement and rethrow exceptions occuring
     */
    private static void closeStatement(Statement statement)
    {
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (SQLException e)
            {
                throw new RuntimeException("Unable to close SQL statement resource.", e);
            }
        }
    }
}

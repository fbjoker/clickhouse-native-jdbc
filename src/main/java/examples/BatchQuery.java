package examples;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 */
public class BatchQuery {

    public static void main(String[] args) throws Exception {

        int fieldNumber = 420;

        String fieldsType = "String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, String, String, " +
                "String, String, String, String, String, String, String, String, String, String, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, " +
                "Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Long, Long, Long, Long, " +
                "Long, Double, Double";
        String[] partner= {"YNYDZX","YNYDHW","JS_CMCC_CP","JS_CMCC_CP_ZX","HNYD","SD_CMCC_JN","LNYD","SAXYD"};
        // Initialize client (endpoint, username, password)

        String table=args[0];
        Long iterNum=Long.parseLong(args[1]);
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://10.10.121.213:9000");

        StringBuilder params= new StringBuilder();
        for (int i = 0; i <fieldNumber ; i++) {
            params.append("?,");
        }

        Statement stmt = connection.createStatement();
        stmt.executeQuery("drop table if exists test_jdbc_example");
        stmt.executeQuery("create table test_jdbc_example(day Date, name String, age UInt8) Engine=Log");

        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO test_jdbc_example VALUES(?, ?, ?)");

        for (int i = 0; i < 20000; i++) {
            pstmt.setDate(1, new Date(System.currentTimeMillis()));
            pstmt.setString(2, "Zhang San" + i);
            pstmt.setByte(3, (byte)i);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        //stmt.executeQuery("drop table test_jdbc_example");
    }
}

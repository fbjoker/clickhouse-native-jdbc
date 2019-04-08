package examples;

import org.apache.commons.lang3.RandomStringUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static examples.utils.DataUtils.getRandomIp;
import static examples.utils.DataUtils.nextTime;

public class Httptest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
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
        String[] partners = {"YNYDZX", "YNYDHW", "JS_CMCC_CP", "JS_CMCC_CP_ZX", "HNYD", "SD_CMCC_JN", "LNYD", "SAXYD"};
        // Initialize client (endpoint, username, password)

        String table = args[0];
        Long iterTime = Long.parseLong(args[1]);
        String partner=partners[new Random().nextInt(8)];
        Long iterNum=1000L;
        for (int i = 0; i <iterTime ; i++) {

            producer2InsertClickHouse(fieldNumber, fieldsType,partner , table, iterNum);
        }

    }


    private static void producer2InsertClickHouse(int fieldNumber, String fieldsType, String partner, String table, Long iterNum) throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://10.10.121.213:8123/test";
        Connection connection = null;
        Statement statement = null;
        ResultSet results = null;
        connection = DriverManager.getConnection(address);
        statement = connection.createStatement();

        StringBuilder params = new StringBuilder();
        for (int i = 0; i < fieldNumber - 1; i++) {
            params.append("?,");
        }
        String param = params.append("?").toString();



        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + table + " VALUES(" + param + ")");

        long startCreatData=System.currentTimeMillis();
        for (int iter = 0; iter < iterNum; iter++) {
            pstmt.setInt(1, ThreadLocalRandom.current().nextInt(100000));
            pstmt.setString(2, partner);
            pstmt.setString(3, new SimpleDateFormat("yyyy-MM-dd ").format(nextTime()));
            pstmt.setString(4, getRandomIp());

            String[] fieldsTypeList = fieldsType.split(",");
            for (int i = 4; i < fieldNumber; i++) {
                switch (fieldsTypeList[i -4].trim()) {
//                case "String":data[i]="abcdefg";
                    case "String":
                        pstmt.setString(i+1, RandomStringUtils.randomAlphanumeric(new Random().nextInt(8) + 1));
                        break;
                    case "Int":
                        pstmt.setInt(i+1, ThreadLocalRandom.current().nextInt(10000));
                        break;
                    case "Long":
                        pstmt.setLong(i+1, ThreadLocalRandom.current().nextLong(1000000));
                        break;
                    case "Double":
                        pstmt.setDouble(i+1, ThreadLocalRandom.current().nextDouble(500));
                        break;
                    default:
                        pstmt.setByte(i+1, Byte.parseByte("1"));
                        break;
                }
            }
            pstmt.addBatch();
        }
        long endCreatData = System.currentTimeMillis();
        System.out.println("本批次生成数据时间："+(endCreatData-startCreatData));
        long startInsert = System.currentTimeMillis();

        pstmt.executeBatch();
        long endInsert = System.currentTimeMillis();
        System.out.println("本批次插入时间："+(endInsert-startInsert));
        //stmt.executeQuery("drop table test_jdbc_example");
    }
}

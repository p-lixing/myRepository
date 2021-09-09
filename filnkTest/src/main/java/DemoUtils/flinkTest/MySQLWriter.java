package DemoUtils.flinkTest;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLWriter
        extends RichSinkFunction<Info>
{

    private Connection connection;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String className = "com.mysql.jdbc.Driver";
        Class.forName(className);
        String url = "jdbc:mysql://localhost:3306/flink";
        String user = "root";
        String password = "123456";
        connection = DriverManager.getConnection(url, user, password);
        String insert = "insert into flinkjson(id,name,sex,score) values(?,?,?,?)";
        String update = "update into flinkjson(id,name,sex,score) values(?,?,?,?)";
        insertStmt = connection.prepareStatement(insert);
        updateStmt = connection.prepareStatement(update);
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (insertStmt != null) {
            insertStmt.close();
        }
        if (updateStmt != null) {
            updateStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    public void invoke(Info value, Context context) throws Exception {
        int id = value.id;
        String name = value.name;
        String sex = value.sex;
        Float score = value.score;
        updateStmt.setInt(1, id);
        updateStmt.setString(2, name);
        updateStmt.setString(3,sex);
        updateStmt.setFloat(4,score);
        int i = updateStmt.executeUpdate();
        if (i == 0) {
            insertStmt.setInt(1, id);
            insertStmt.setString(2, name);
            insertStmt.setString(3,sex);
            insertStmt.setFloat(4,score);
        }
    }
}

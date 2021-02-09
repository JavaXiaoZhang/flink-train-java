package com.flink.sink;

import com.flink.dataset.Student;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkToMysql extends RichSinkFunction<Student> {

    Connection connection = null;
    PreparedStatement preparedStatement = null;

    static SimpleAccumulator counter = new IntCounter();

    private Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager
                .getConnection("jdbc:mysql://localhost:3306/flink?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC"
                        ,"root","1234");
    }

    /**
     * 生命周期，执行次数与并行度有关
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        preparedStatement = connection.prepareStatement("insert into student values(?,?,?,?)");
        getRuntimeContext().addAccumulator("id", counter);
    }

    /**
     * 每条记录执行一次
     * @param student
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student student, Context context) throws Exception {
        preparedStatement.setInt(1,(Integer) counter.getLocalValue());
        preparedStatement.setString(2,student.getName());
        preparedStatement.setInt(3,student.getAge());
        preparedStatement.setString(4,student.getGender());
        preparedStatement.executeUpdate();
        counter.add(1);
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }

        if (connection != null) {
            connection.close();
        }
    }
}

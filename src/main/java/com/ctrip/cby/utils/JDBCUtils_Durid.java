package com.ctrip.cby.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtils_Durid {
    static DataSource dataSource =null;
    static ThreadLocal<Connection> tl = new ThreadLocal<>();
    static {
        Properties pro = new Properties();
        InputStream in = JDBCUtils_Durid.class.getClassLoader().getResourceAsStream("druid.properties");
        try {
            pro.load(in);
            dataSource = DruidDataSourceFactory.createDataSource(pro);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //    获取连接
    public static Connection getConnection() throws SQLException {
        Connection con = tl.get();
        if (con == null) {
            con = dataSource.getConnection();
            tl.set(con);
        }
        return  con;
    }


    //    释放连接
    public static void closeResource(ResultSet rs, Statement st, Connection con) throws SQLException {
        if (rs != null) {
            rs.close();
        }

        if (st != null) {
            st.close();
        }

        if (con != null) {
            tl.remove();
            con.close();
        }
    }
}


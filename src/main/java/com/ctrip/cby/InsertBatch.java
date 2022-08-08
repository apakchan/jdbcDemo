package com.ctrip.cby;

import com.ctrip.cby.utils.JDBCUtils_Durid;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author baiyuchen
 */
public class InsertBatch {
    /**
     * 表名
     */
    private static final String TABLE_NAME = "single_table";

    /**
     * 插入的条数
     */
    private static final int NUM = 10000;

    @Test
    public void test(){
        Random r = new Random();
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = JDBCUtils_Durid.getConnection();
            ps = con.prepareStatement("INSERT INTO "
                    + TABLE_NAME + "(key1, key2, key3, key_part1, key_part2, key_part3, common_field)"
                    + " values(?, ?, ?, ?, ?, ?, ?)");
            for (int i = 0; i < NUM; i++) {
                ps.setString(1, String.valueOf(r.nextInt(10000)));
                ps.setInt(2, i);
                ps.setString(3, String.valueOf(r.nextInt(10000)));
                ps.setString(4, String.valueOf(r.nextInt(10000)));
                ps.setString(5, String.valueOf(r.nextInt(10000)));
                ps.setString(6, String.valueOf(r.nextInt(10000)));
                ps.setString(7, String.valueOf(r.nextInt(10000)));
                ps.addBatch();

                // 如果除不尽的话，最后一部分不足3000的数据是不会被写入的
                if(i % 3000 == 0){
                    System.out.println(i);
                    ps.executeBatch();
                }
            }
            // 处理最后一部分小于除数的数据
            ps.executeBatch();
            ps.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            // 关闭连接
            try {
                JDBCUtils_Durid.closeResource(null, ps, con);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

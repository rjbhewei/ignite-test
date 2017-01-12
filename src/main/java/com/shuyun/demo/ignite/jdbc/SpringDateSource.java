package com.shuyun.demo.ignite.jdbc;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/5  18:50
 * @desc
 */
public class SpringDateSource {

    private static class HEWEI {

        private static final DriverManagerDataSource dataSource = new DriverManagerDataSource();

        private static final String dirverClassName = "com.mysql.jdbc.Driver";

        private static final String url = "jdbc:mysql://172.18.2.37:3306/channel";

        private static final String user = "development";

        private static final String pswd = "development";

        static {
            dataSource.setDriverClassName(dirverClassName);
            dataSource.setUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(pswd);
        }
    }

    public static DriverManagerDataSource dataSource() {
        return HEWEI.dataSource;
    }
}

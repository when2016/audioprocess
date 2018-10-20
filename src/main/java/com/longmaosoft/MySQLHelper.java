package com.longmaosoft;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLHelper {
    private static final String dbUserName = "root";
    private static final String dbUserPassword = "123456";
    private static final String dbUrl = "jdbc:mysql://localhost:3306/test?tinyInt1isBit=false&amp;useUnicode=true&amp;characterEncoding=utf-8";
    private static AtomicInteger nextIdx = new AtomicInteger(-1);
    private static Map<String, List<String>> columnNameCache = new HashMap();
    private static List<DataSource> readOnlyDataSources = new ArrayList();
    private static HikariDataSource masterDataSource = null;

    private static void init() {
        if (masterDataSource != null) {
            masterDataSource.close();
        }
        masterDataSource = initDataSource("jdbc:mysql://localhost:3306/test?tinyInt1isBit=false&amp;useUnicode=true&amp;characterEncoding=utf-8");
    }

    private static void initReadOnly() {
        for (DataSource dataSource : readOnlyDataSources) {
            if ((dataSource instanceof HikariDataSource)) {
                ((HikariDataSource) dataSource).close();
            }
        }
        String readOnlyUrls = "jdbc:mysql://localhost:3306/test?tinyInt1isBit=false&amp;useUnicode=true&amp;characterEncoding=utf-8";
        if (StringUtils.isBlank(readOnlyUrls)) {
            return;
        }
        String[] urls = StringUtils.split(readOnlyUrls, ",");
        for (String url : urls) {
            HikariDataSource ds = initDataSource(url);
            if (ds != null) {
                readOnlyDataSources.add(ds);
            }
        }
    }

    private static HikariDataSource initDataSource(String dbUrl) {
        HikariDataSource ds = null;
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbUrl);
            config.setDriverClassName("com.mysql.jdbc.Driver");
            config.setUsername("root");
            config.setPassword("123456");
            config.setAutoCommit(true);
            config.setConnectionTimeout(NumberUtils.toLong("", 30000L));
            config.setIdleTimeout(NumberUtils.toLong("", 60000L));
            config.setMaxLifetime(NumberUtils.toLong("", 0L));
            config.setMaximumPoolSize(5);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            ds = new HikariDataSource(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ds;
    }

    static {
        init();
        initReadOnly();
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception localException) {
        }
    }

    private static Connection getDirectConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/test?tinyInt1isBit=false&amp;useUnicode=true&amp;characterEncoding=utf-8", "root", "123456");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Connection getMasterConnection() {
        return getDirectConnection();
    }

    public static Connection getReadOnlyConnection() {
        return getDirectConnection();
    }

    public static <T> T executeQuery(MySQLCallback<T> callback, String sql, Object... params)
            throws Exception {
        Connection connection = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            connection = getReadOnlyConnection();
            pst = connection.prepareStatement(sql, 1005, 1008);
            int index = 0;
            if (params != null) {
                for (Object param : params) {
                    pst.setObject(++index, param);
                }
            }
            rs = pst.executeQuery();
            if (callback != null) {
                return (T) callback.executeQuery(rs);
            }
        } finally {
            close(rs);
            close(pst);
            close(connection);
        }
        return null;
    }

    public static int executeUpdate(String sql, Object... params)
            throws SQLException {
        Connection connection = null;
        PreparedStatement pst = null;
        try {
            connection = getMasterConnection();
            pst = connection.prepareStatement(sql, 1005, 1008);
            int index = 0;
            for (Object param : params) {
                pst.setObject(++index, param);
            }
            return pst.executeUpdate();
        } finally {
            close(pst);
            close(connection);
        }
    }

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Statement st) {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static List<String> getColumns(ResultSet rs, String sql) {
        String s = StringUtils.substringBefore(sql, "where").trim();
        List<String> columns = (List) columnNameCache.get(s);
        if ((columns == null) || (columns.isEmpty())) {
            columns = new ArrayList();
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(metaData.getColumnName(i));
                }
                columnNameCache.put(s, columns);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return columns;
    }

    public static Map findFirst(final String sql, String... args) {
        try {
            return executeQuery(new MySQLCallback<Map>() {
                @Override
                public Map executeQuery(ResultSet rs) throws Exception {
                    List<String> columns = getColumns(rs, sql);
                    Map<String, Object> data = null;
                    if (rs.next()) {
                        data = new HashMap<String, Object>();
                        for (int i = 1; i <= columns.size(); i++) {
                            data.put(columns.get(i - 1), rs.getObject(i));
                        }
                    }
                    return data;
                }
            }, sql, args);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<Map> find(final String sql, Object... args) {
        try {
            return executeQuery(new MySQLCallback<List<Map>>() {
                @Override
                public List<Map> executeQuery(ResultSet rs) throws Exception {
                    List<String> columns = getColumns(rs, sql);
                    List<Map> data = new ArrayList<Map>();
                    while (rs.next()) {
                        Map<String, Object> map = new HashMap<String, Object>();
                        for (int i = 1; i <= columns.size(); i++) {
                            map.put(columns.get(i - 1), rs.getObject(i));
                        }
                        data.add(map);
                    }
                    return data;
                }
            }, sql, args);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<DataSource> getReadOnlyDataSources() {
        return readOnlyDataSources;
    }

    public static abstract interface MySQLCallback<T> {
        public abstract T executeQuery(ResultSet paramResultSet)
                throws Exception;
    }
}

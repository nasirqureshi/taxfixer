package fixtax2015;

// ~--- JDK imports ------------------------------------------------------------

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BvasConFatory {
  private static final String LOCAL_NY_TAXDB =
      "jdbc:mysql://192.168.3.244:3306/bvasdb?zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8&user=root&password=001FS740";
  private static final String LOCAL_DB =
      "jdbc:mysql://localhost:3306/bvasdb?zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8&user=root&password=001FS740";
  private static final String CHTAX_DB =
      "jdbc:mysql://192.168.1.233:3306/bvasdb?zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8&user=root&password=001FS740";

  private String jdbcDriver;
  private String mysqlUrl;

  public BvasConFatory(String dbpointer) {
    if (dbpointer.equalsIgnoreCase("local")) {
      this.jdbcDriver = "com.mysql.jdbc.Driver";
      this.mysqlUrl = LOCAL_DB;
    } else if (dbpointer.equalsIgnoreCase("localtaxny")) {
      this.jdbcDriver = "com.mysql.jdbc.Driver";
      this.mysqlUrl = LOCAL_NY_TAXDB;
    } else if (dbpointer.equalsIgnoreCase("chtaxdb")) {
      this.jdbcDriver = "com.mysql.jdbc.Driver";
      this.mysqlUrl = CHTAX_DB;
    } else {
      this.mysqlUrl = "";
    }
  }

  public Connection getConnection() throws SQLException {
    Connection con = null;

    try {
      Class.forName(jdbcDriver).newInstance();
      con = DriverManager.getConnection(mysqlUrl);
    } catch (InstantiationException ex) {
      System.out.println("Exception---" + ex);
    } catch (IllegalAccessException ex) {
      System.out.println("Exception---" + ex);
    } catch (ClassNotFoundException ex) {
      System.out.println("Exception---" + ex);
    } catch (SQLException ex) {
      System.out.println("Exception---" + ex);
    }

    return con;
  }

  public void setMysqlUrl(String mysqlUrl) {
    this.mysqlUrl = mysqlUrl;
  }

  public String getMysqlUrl() {
    return mysqlUrl;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }
}

// ~ Formatted by Jindent --- http://www.jindent.com

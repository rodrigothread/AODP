/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.exceptions.dataSource.CatalogException;
import ch.epfl.codimsd.qeef.util.Constants;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author douglas
 */
public class UpdateDB {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Connection con;
        Statement stmt;

        // TODO code application logic here
        String params = "org.postgresql.Driver;jdbc:postgresql://146.134.150.35:5432/pretoria_all;postgres;adm123";
               // String params = "org.postgresql.Driver;jdbc:postgresql://146.134.235.1:5432/qefdb;postgres;adm123";
        // String params = "oracle.jdbc.driver.OracleDriver;jdbc:oracle:thin:webb@//dexl06:1521/producao;webb;webb";

        // Parse the string containing catalog connection parameters
        int numberOfDelimiter = 0;
        for (int i = 0; i < params.length(); i++) {
            if (params.charAt(i) == ';') {
                numberOfDelimiter++;
            }
        }

        String driverName = null, url = null, user = null, pwd = null;
        String[] connectionParams = new String[numberOfDelimiter + 1];
        connectionParams = params.split(";", numberOfDelimiter + 1);
        Properties properties = new Properties();

        driverName = connectionParams[0];
        url = connectionParams[1];
        user = connectionParams[2];
        pwd = connectionParams[3];

        // Load the driver, create the connection/statement objects
        properties.setProperty("user", user);
        properties.setProperty("password", pwd);
        try {

            Class.forName(driverName);
            con = DriverManager.getConnection(url, properties);
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            String query = "SELECT coadd_objects_id, flag2, flag4, flag6, flag8, flag10, flag20, flag40 FROM dr012_coadd_objects_tag WHERE flag2 IS NULL";
            ResultSet rset = null;
            rset = stmt.executeQuery(query);

            int flag2 = 2;
            int c2 = 8662037;
            int flag4 = 4;
            int c4 = 1215185;
            int flag6 = 5;
            int c6 = 3697470;
            int flag8 = 7;
            int c8 = 1215182;
            int flag10 = 8;
            int c10 = 2704551;
            int flag20 = 16;
            int c20 = 1215173;
            int flag40 = 32;
            int c40 = 470472;
            while (rset.next()) {
                rset.updateInt("flag2", flag2);
                rset.updateInt("flag4", flag4);
                rset.updateInt("flag6", flag6);
                rset.updateInt("flag8", flag8);
                rset.updateInt("flag10", flag10);
                rset.updateInt("flag20", flag20);
                rset.updateInt("flag40", flag40);
                rset.updateRow();
                c2++;
                c4++;
                c6++;
                c8++;
                c10++;
                c20++;
                c40++;
                if (c2 == 14893701) {
                    flag2++;
                    c2 = 0;
                }
                if (c4 == 7446851) {
                    flag4++;
                    c4 = 0;
                }
                if (c6 == 4964567) {
                    flag6++;
                    c6 = 0;
                }
                if (c8 == 3723426) {
                    flag8++;
                    c8 = 0;
                }
                if (c10 == 2978741) {
                    flag10++;
                    c10 = 0;
                }
                if (c20 == 1489371) {
                    flag20++;
                    c20 = 0;
                }
                if (c40 == 744686) {
                    flag40++;
                    c40 = 0;
                }
                if (c2 % 1000 == 0) {
                    System.out.println(c2);
                }
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(UpdateDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(UpdateDB.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}

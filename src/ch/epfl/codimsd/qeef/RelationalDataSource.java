
/*
 * CoDIMS version 1.0 
 * Copyright (C) 2006 Othman Tajmouati
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package ch.epfl.codimsd.qeef;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.apache.log4j.Logger;

import ch.epfl.codimsd.connection.TransactionMonitor;
import ch.epfl.codimsd.exceptions.dataSource.CatalogException;
import ch.epfl.codimsd.exceptions.dataSource.DataSourceException;
import ch.epfl.codimsd.qeef.relational.Column;
import ch.epfl.codimsd.qeef.relational.Tuple;
import ch.epfl.codimsd.qeef.relational.TupleMetadata;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
//import org.apache.hadoop.fs.FSDataInputStream;
import java.util.Scanner;

/**
 * Version 1
 *
 * RelationalDataSource reads the datasources from a given Database. It executes
 * an sql query on the database and returns tuple objects.
 *
 * @author Othman Tajmouati
 *
 * @date May 24, 2006
 */
public class RelationalDataSource extends DataSource {

    /**
     * Sql ResultSet object storing the tuples.
     */
    private ResultSet rset = null;
    private Connection con;
    /**
     * Sql ResultSet Metadata object.
     */
    private ResultSetMetaData rsetMetadata;

    /**
     * Number of columns in each row.
     */
    private int numberOfColumns = 4;
    private final String BD;
    private Tuple tuple;

    private TransactionMonitor tm;
    private int idBD;
    private int countTuplas = 0;
    private Scanner leitor;
    private String query;
    private String arq;
    File arquivo;

    /**
     * Log4j Logger.
     */
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(RelationalDataSource.class.getName());

    /**
     * Default constructor
     *
     * @param alias the name of this DataSource, currently set to
     * "RelationalDataSource
     * @param metadata metadata associated with this dataSource. Metadata
     * defines the format of sql types within each column of a jdbc ResultSet
     * @param BD
     * @throws ch.epfl.codimsd.exceptions.dataSource.CatalogException
     */
    public RelationalDataSource(String alias, TupleMetadata metadata, String BD)
            throws CatalogException {

        // Call super constructor.
        super(alias, null);
        this.BD = BD;
    }

    public void open() throws DataSourceException, FileNotFoundException, ClassNotFoundException {

        BlackBoard bb = BlackBoard.getBlackBoard();

        arquivo = new File(BD);

        // Put Metadata of the dataSource in the BlackBoard.           
        metadata = new TupleMetadata();

        Column column = new Column("nomedoarquivo", Config.getDataType(Constants.STRING), 1, 1, false);
        metadata.addData(column);

        column = new Column("Particoes", Config.getDataType(Constants.STRING), 1, 2, false);
        metadata.addData(column);
        numberOfColumns = 2;
        tuple = new Tuple();
        setMetadata(metadata);
        bb.put("Metadata", metadata);

    }

    /**
     * Read a specific row from a jdbc ResultSet object and return a tuple
     * containing the row.
     *
     * @return a DataUnit containing OracleType.
     * @throws ch.epfl.codimsd.exceptions.dataSource.DataSourceException
     */
    public DataUnit read() throws DataSourceException, SQLException, FileNotFoundException, IOException {
        // Construct a new tuple where we insert our sql object.
        tuple.removeAllData();

        BufferedReader in = new BufferedReader(new FileReader(arquivo));

        while (in.ready()) {

            countTuplas++;
            String str = in.readLine();

            String s[] = str.split(",");
                //System.out.println(s[0]);

            // Construct the OracleType object before sending to Scan operator.
            for (int i = 1; i <= numberOfColumns; i++) {
                StringType Object = new StringType(s[i - 1]);
                tuple.addData(Object);

                //System.out.println(Object);
            }
            return tuple;

        }

        return null;
    }

    public void close() throws DataSourceException {

    }
}

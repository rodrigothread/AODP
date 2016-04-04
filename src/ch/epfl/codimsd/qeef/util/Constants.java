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
package ch.epfl.codimsd.qeef.util;

import java.io.File;

/**
 * The Constants class store CoDIMS constants.
 *
 * @author Othman Tajmouati.
 */
public interface Constants {

    // Initial Configuration parameters
    static final String HOME = "CODIMS_HOME";
    static final String LOG4J_HOME_PROP = "codims.home";
    static final String LOG4J_FILENAME_PROP = "log4j.filename";
    static final String LOG4J_FILENAME = "codimsLogs";

    // Parameters written to the Catalog
    static final String QEPInitial = "QEPInitial";
    static final String IRI_DATABASECATALOG = "ds_database";
    static final String IRI_TABLECATALOG = "ds_table";

    // Objects taken from the BlackBoard
    static final String TRANSACTION_MONITOR = "TRANSACTION_MONITOR";
    static final String DATASOURCE_MANAGER = "DATASOURCE_MANAGER";

    // Operator names and parameters 
    static final String QEP_DATASOURCE_NAME_PARAM = "DataSourceName";
    static final String QEP_SCAN_NUMBER_TUPLES = "numberTuples";

    // These parameters are used in order to access the QEP
    // If the QEP is a local one then it's an xml file otherwise it's a text file
    static final String qepAccessTypeLocal = "LOCAL";

    //Parameters for table time in hadoop Tests
    static final String QUERY_TIME = "QUERY_TIME";
    static final String WRITE_TIME = "WRITE_TIME";
    static final String SKYMAP_TIME = "SKYMAP_TIME";

    // DB parameters
    static final String COMMIT = "COMMIT";
    static final String ROLLBACK = "ROLLBACK";
    static final String SELECT = "SELECT";
    static final String INSERT = "INSERT";
    static final String UPDATE = "UPDATE";
    static final String DELETE = "DELETE";
    static final String SQLDB = "SQLDB";

    // Execution modes and multi-env execution informations
    static final int SINGLE_USER = 0;
    static final int MULTI_USER = 1;
    static final String REQUEST_ID = "REQUEST_ID";
    static final int REQUEST_YES = 1;
    static final int REQUEST_NO = 0;
    static final String LOG_EXECUTION_PROFILE = "LOG_EXECUTION_PROFILE";
    static final String NO_DISTRIBUTION = "NO_DISTRIBUTION";

    // Thread names
    static final String timeOutThread = "TIMEOUT_CHECKER";
    static final String QueryManagerThread = "QUERY_MANAGER_THREAD";

    // Codims Types
    static final String ORACLETYPE = "ORACLE_TYPE";
    static final String STRING = "STRING";
    static final String INTEGER = "INTEGER";
    static final String FLOAT = "FLOAT";
    static final String DOUBLE = "DOUBLE";

    // Request types
    static final int REQUEST_TYPE_SERVICE_WEBB = 0;
    static final int REQUEST_TYPE_SERVICE_SKYMAP = 1;
    static final int REQUEST_TYPE_SERVICE_SKYMAPADD = 2;

    //BDs Data
    static final String BD_ORIGEM_USER = "BD_ORIGEM_USER";
    static final String BD_DESTINO_USER = "BD_DESTINO_USER";
    static final String BD_WEBB_USER = "BD_WEBB_USER";
    static final String BD_ORIGEM_PWD = "BD_ORIGEM_PWD";
    static final String BD_DESTINO_PWD = "BD_DESTINO_PWD";
    static final String BD_WEBB_PWD = "BD_WEBB_PWD";
    static final String BD_ORIGEM_URL = "BD_ORIGEM_URL";
    static final String BD_DESTINO_URL = "BD_DESTINO_URL";
    static final String BD_WEBB_URL = "BD_WEBB_URL";
    static final String BD_IRI = "BD_IRI";

    static final String DATA_HDFS_PART = "DATA_HDFS_PART";
}

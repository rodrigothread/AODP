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

import ch.epfl.codimsd.connection.DerbyServerStarter;
import ch.epfl.codimsd.connection.CatalogManager;
import ch.epfl.codimsd.connection.TransactionMonitor;
import ch.epfl.codimsd.exceptions.CodimsException;
import ch.epfl.codimsd.exceptions.dataSource.CatalogException;
import ch.epfl.codimsd.exceptions.initialization.InitializationException;
import ch.epfl.codimsd.exceptions.shutdown.ShutDownException;
import ch.epfl.codimsd.exceptions.transactionMonitor.TransactionMonitorException;

import ch.epfl.codimsd.query.RequestParameter;
import ch.epfl.codimsd.query.RequestResult;
import ch.epfl.codimsd.query.Request;
import ch.epfl.codimsd.qeef.util.Constants;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Version 1
 *
 * QueryManagerImpl is the entry point of CoDIMS-D. In order to use CoDIMS for
 * request execution, the user should perform the following :
 * <li> Initialize CoDIMS by calling @see
 * ch.epfl.codimsd.qeef.QueryManagerImpl#getQueryManagerImpl()
 * <li> Execute the request : - Synchronous request : @see
 * ch.epfl.codimsd.qeef.QueryManagerImpl#executeRequest(Request) - Asynchronous
 * request : @see @see
 * ch.epfl.codimsd.qeef.QueryManagerImpl#executeAsync(Request)
 * <li> Shutdown CoDIMS : @see @see
 * ch.epfl.codimsd.qeef.QueryManagerImpl#shutdown()
 *
 * @author Othman Tajmouati
 *
 * @date April 13, 2006
 */
public class QueryManagerImpl extends Thread {

    /**
     * Log4j logger.
     */
    @SuppressWarnings("unused")
    private Logger logger = Logger.getLogger(QueryManagerImpl.class.getName());

    /**
     * The TransactionMonitor
     */
    private TransactionMonitor transactionMonitor;

    /**
     * The CatalogManager object handles the communication with the catalog
     * database.
     */
    private CatalogManager catalogManager;

    /**
     * The DiscoverQueryManagerImpl creates a singleton object (as defined in
     * the Design Pattern).
     */
    private static QueryManagerImpl ref = null;

    /**
     * Private default constructor. It calls initializeService().
     *
     * @throws CodimsException
     */
    private QueryManagerImpl() throws CodimsException {
        initializeService();
    }

    /**
     * The getQueryManagerImpl() creates a QueryManagerImpl singleton, ensuring
     * the unicity of the object during the system execution time (see Design
     * Pattern for more informations
     * {@link http://java.sun.com/blueprints/patterns/})
     *
     * @return the singleton object
     *
     * @throws CodimsException
     */
    public static synchronized QueryManagerImpl getQueryManagerImpl()
            throws CodimsException {

        // Call one single reference of the class.
        if (ref == null) {
            ref = new QueryManagerImpl();
        }

        return ref;
    }

    /**
     * initializeService loads the system Configuration, initializes the
     * BlackBoard and inserts the TransactionMonitor into the BlackBoard
     *
     * @throws CodimsException
     */
    private synchronized void initializeService() throws CodimsException {

        // Loads catalog connection parameters and initializes the CatalogManager
        SystemConfiguration.loadSystemConfiguration();

        try {

            // Initializes the Catalog.
            catalogManager = CatalogManager.getCatalogManager();

            // Intializes the TransactionMonitor.
            transactionMonitor = TransactionMonitor.getTransactionMonitor();

        } catch (CatalogException ex) {
            throw new InitializationException("Error in Catalogmanager : " + ex.getMessage());
        } catch (TransactionMonitorException ex) {
            throw new InitializationException("InitializationException : " + ex.getMessage());
        } catch (Exception ex) {
            throw new InitializationException("Error in DerbyServerStarter : " + ex.getMessage());
        }
    }

    /**
     * executeRequest computes the request (according to the requestType) and
     * returns the result in a RequestResult object.
     *
     * @param request a request of type Request
     * @return requestResult contains the result returned for this request
     */
    public synchronized RequestResult executeRequest(Request request) throws CodimsException {

        // Set the request id to 1
        request.setRequestID((long) 1);

        // RequestResult contains the final result.
        RequestResult requestResult = null;

        // Intializes the main execution component and execute the request.
        QueryManager queryManager = new QueryManager(request);
        queryManager.executeRequest();

        // Get the results.
        requestResult = queryManager.getRequestResult();

        // Return the results.
        return requestResult;
    }

    /**
     * Shutdown the QueryManagerImpl class. It disconnects the connections with
     * the CatalogManager and the TransactionMonitor
     *
     * @throws ShutDownException
     */
    public synchronized void shutdown() throws CodimsException {

        try {
            // Close CoDIMS main component,
            catalogManager.closeCatalogManager();
            transactionMonitor.closeTransactionMonitor();

        } catch (TransactionMonitorException ex) {
            throw new CodimsException("Cannot close TransactionMonitor : " + ex.getMessage());
        }
    }

    /**
     * Returns the request result.
     *
     * @param requestId
     * @return the request result object or null if the execution is not
     * finished.
     */
    public synchronized RequestResult getRequestResult(long requestId) {

        BlackBoard bl = BlackBoard.getBlackBoard();

        if (bl.containsKey("REQUEST_ID_" + requestId)) {
            return (RequestResult) bl.get("REQUEST_ID_" + requestId);
        } else {
            return null;
        }
    }

    /**
     * @param args
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void main(String args[]) throws Exception {
        long inicioTempo= System.currentTimeMillis();
        //Logger logger = Logger.getLogger(QueryManagerImpl.class.getName());
        QueryManagerImpl queryManagerImpl = QueryManagerImpl.getQueryManagerImpl();
        CatalogManager cm = CatalogManager.getCatalogManager();

        int requestType = Constants.REQUEST_NO;

        BlackBoard bb = BlackBoard.getBlackBoard();
        bb.put(Constants.BD_IRI, "SourceBD");
        bb.put(Constants.SQLDB, "select * from databases");

        RequestParameter requestParameter = new RequestParameter();
        requestParameter.setParameter(Constants.QEPInitial, "TRUE");
        requestParameter.setParameter(Constants.NO_DISTRIBUTION, "TRUE");

        Request request = new Request(null, requestType, requestParameter);

        RequestResult finalRequestResult = queryManagerImpl.executeRequest(request);
        
        System.out.println("FINAL APPLICATTION TIME = "+(System.currentTimeMillis()-inicioTempo)/1000+" Segundos");
        
        System.exit(0);
    }

}

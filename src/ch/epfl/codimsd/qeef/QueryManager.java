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

import java.rmi.RemoteException;

import javax.activation.DataHandler;

import org.apache.log4j.Logger;

import ch.epfl.codimsd.exceptions.CodimsException;
import ch.epfl.codimsd.exceptions.distributed.DistributedException;
import ch.epfl.codimsd.qep.QEP;
import ch.epfl.codimsd.query.QueryComponent;
import ch.epfl.codimsd.query.Request;
import ch.epfl.codimsd.query.RequestResult;

/**
 * Version 1
 *
 * QueryManager is the base class managing the execution of a request. It is
 * called by QueryManagerImpl in its executeRequest or execAsnyc methods.
 *
 * @author Othman Tajmouati
 *
 * @date April 13, 2006
 */
public class QueryManager extends Thread {

    /**
     * Log4j logger.
     */
    private Logger logger = Logger.getLogger(QueryManager.class.getName());

    /**
     * The request to execute.
     */
    private Request request;

    /**
     * RequestResult containing the results of this request.
     */
    private RequestResult requestResult;

    /**
     * Default constructor
     */
    public QueryManager(Request request) {

        this.request = request;
    }

    /**
     * executRequest computes the request sent by the QueryManagerImpl. The
     * query execution performs the following actions :
     * <li> Calls the QPCompFactory which creates (according the request type)
     * the corresponding object instances for the system
     * <li> Encapsulate these objects in a QueryComponent
     * <li> Calls the DiscoveryPlanManager which creates the query execution
     * plan (QEP)
     * <li> Executes the QEP in distributed or centralized mode.
     * <li> Returns the results in a RequestResult object
     *
     * @param request the request (containing the requestType and the
     * requestParameter fields)
     * @return requestResult the result of execution
     *
     * @throws RemoteException
     * @throws CodimsException
     */
    public synchronized void executeRequest() throws CodimsException {

        PerformanceAnalyzer pa = PerformanceAnalyzer.getPerformanceAnalyzer();

        long buildingPlanAndComponentTime = System.currentTimeMillis();

        // Call the QPCompFactory, and intialize the required component.
        QPCompFactory qpCompFactory = new QPCompFactory(request);
        QueryComponent queryComponent = qpCompFactory.getQueryComponent();

        // Create and optimize the QEP.
        PlanManager planManager = queryComponent.getPlanManager();

        QEP qep = (QEP) planManager.instantiatePlan(request);

//		pa.log("Building components and QEP plan in client side  : ", System.currentTimeMillis()-buildingPlanAndComponentTime);
        // If there exist remote nodes, we execute the request in distributed mode.
        logger.debug("Executing the request.");
        long beginTime = System.currentTimeMillis();
        executeLocalRequest(qep);

        // Log performance times and create the log file.
//		pa.log("Total Execution time for Request (id " + request.getRequestID() + ")  : ", System.currentTimeMillis()-beginTime);
        //	pa.finalizeLogging();
        logger.debug("Execution finished.");
    }

    /**
     * Call requestResult in this Thread.
     */
    public void run() {

        try {
            executeRequest();
            this.interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return the RequestResult.
     */
    public RequestResult getRequestResult() {
        return requestResult;
    }

    public void executeLocalRequest(QEP qep) throws CodimsException {

        // Execute the request in centralized mode.
        QEEF qeef = new QEEF();
        qeef.setPlanOperator(qep.getConcreteopList());
        try {
            long beginExecutionTime = System.currentTimeMillis();
            requestResult = qeef.execute();
            requestResult.setElapsedTime(System.currentTimeMillis() - beginExecutionTime);

        } catch (Exception ex) {
            throw new CodimsException(ex.getMessage());
        }
    }
}

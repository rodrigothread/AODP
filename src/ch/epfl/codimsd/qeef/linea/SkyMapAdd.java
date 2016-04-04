/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qep.OpNode;

/**
 *
 * @author douglas
 */
public class SkyMapAdd extends Operator {

    public SkyMapAdd(int id, OpNode opNode) throws Exception {
        super(id);

    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();
    }

    public void setMetadata(Metadata prdMetadata[]) {
        this.metadata[0] = (Metadata) prdMetadata[0].clone();
    }

    public void close() throws Exception {
        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        return null;
    }
}

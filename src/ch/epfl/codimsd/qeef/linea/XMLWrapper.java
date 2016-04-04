/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qep.OpNode;

import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 *
 * @author douglas
 */
public class XMLWrapper extends Operator {

    public XMLWrapper(int id, OpNode opNode) throws Exception {
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

        buildOpNodeTable();

        return null;
    }

    private static void buildOpNodeTable() throws DocumentException {

        // Call Sax reader in order to parse the xml document.
        SAXReader reader = new SAXReader();
        Document document = reader.read("/home/douglas/QEF_Hadoop/pypeline/input.xml");

        List list = document.selectNodes("//file");
        Iterator itt = list.iterator();

        while (itt.hasNext()) {

            Element opElement = (Element) itt.next();

            // Get operator ID from xml QEP
            System.out.println("elemente = " + opElement.attributeValue("value"));
        }
    }
}

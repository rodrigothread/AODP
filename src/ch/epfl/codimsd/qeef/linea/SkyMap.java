package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import java.io.FileWriter;
import java.net.InetAddress;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class SkyMap extends Operator {

    private String params;

    private float pixsize;
    private int ramin;
    private int ramax;
    private int decmin;
    private int decmax;
    private FileWriter fw;
    private StringType object;
    private boolean firstTime = true;
    private long execTime;
    // private int limit;
    private BlackBoard bb;

    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public SkyMap(int id, OpNode opNode) throws Exception {
        super(id);

        pixsize = Float.parseFloat(opNode.getParams()[0]);
        ramin = Integer.parseInt(opNode.getParams()[1]);
        ramax = Integer.parseInt(opNode.getParams()[2]);
        decmin = Integer.parseInt(opNode.getParams()[3]);
        decmax = Integer.parseInt(opNode.getParams()[4]);
        //   limit = 0;
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        bb = BlackBoard.getBlackBoard();
        String part = (String) bb.get(Constants.DATA_HDFS_PART);
        String path = "/tmp/" + hostname + "_" + part;
        fw = new FileWriter(path);

        params = new String();
        params = "/usr/bin/python /home/hduser/Documents/Douglas/QEF_Hadoop/Python/skymap.py " + pixsize
                + " " + ramin + " " + ramax + " " + decmin + " " + decmax + " " + path + " " + "/home/hduser/pkls/" + hostname + "_" + part;
    }

    public void setMetadata(Metadata prdMetadata[]) {
        this.metadata[0] = (Metadata) prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        Instance instance = (Instance) super.getNext(id);

        if (firstTime) {
            execTime = System.currentTimeMillis();
            firstTime = false;
        }

        if (instance == null) {
            fw.close();

            bb.put(Constants.WRITE_TIME, "" + (System.currentTimeMillis() - execTime));

            System.out.println("Tempo de Execução da Gravação de Arquivos : " + (System.currentTimeMillis() - execTime));
            System.out.println("params = " + params);

            execTime = System.currentTimeMillis();
            Process pr = Runtime.getRuntime().exec(params);

            pr.waitFor();
            bb.put(Constants.SKYMAP_TIME, "" + (System.currentTimeMillis() - execTime));
            System.out.println("Tempo de Execução do SkyMap : " + (System.currentTimeMillis() - execTime));

            return null;
        }
        // limit++;

        object = (StringType) instance.getData(0);
        fw.write(object.toString() + ":");

        object = (StringType) instance.getData(1);
        fw.write(object.toString() + ":");

        return instance;
    }
}

package ch.epfl.codimsd.qeef.linea;

import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qeef.types.DoubleType;
import ch.epfl.codimsd.qeef.types.FloatType;
import ch.epfl.codimsd.qeef.types.OracleType;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class SkyMapArq extends Operator {

    private String params;

    private float pixsize;
    private int ramin;
    private int ramax;
    private int decmin;
    private int decmax;
    private FileWriter fw;
    //private DoubleType object;
    //hdfs 
    private StringType object;
    //BD private OracleType object;
    private boolean firstTime = true;
    private long execTime;
    private int limit;

    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public SkyMapArq(int id, OpNode opNode) throws Exception {
        super(id);

        pixsize = Float.parseFloat(opNode.getParams()[0]);
        ramin = Integer.parseInt(opNode.getParams()[1]);
        ramax = Integer.parseInt(opNode.getParams()[2]);
        decmin = Integer.parseInt(opNode.getParams()[3]);
        decmax = Integer.parseInt(opNode.getParams()[4]);
        limit = 0;
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        String path = "/tmp/" + hostname;
        fw = new FileWriter(path);

        params = new String();
        params = "/usr/bin/python /home/hduser/Documents/Douglas/QEF_Hadoop/Python/skymap.py " + pixsize
                + " " + ramin + " " + ramax + " " + decmin + " " + decmax + " " + path + " " + "/home/hduser/Documents/Douglas/QEF_Hadoop/Infos/pkls/" + hostname;
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

        if (instance == null || limit == 2000) {
            fw.close();

            BlackBoard bb = BlackBoard.getBlackBoard();
            bb.put(Constants.WRITE_TIME, "" + (System.currentTimeMillis() - execTime));

            System.out.println("Tempo de Execução da Gravação de Arquivos : " + (System.currentTimeMillis() - execTime));
            System.out.println("params = " + params);

            execTime = System.currentTimeMillis();
            Process pr = Runtime.getRuntime().exec(params);

            /*  String line=null;
             BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

             while((line=input.readLine()) != null) {
             System.out.println(line);
             }

             BufferedReader error = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
             while((line=error.readLine()) != null) {
             System.out.println("ERROR = "+line);
             }*/
            pr.waitFor();
            bb.put(Constants.SKYMAP_TIME, "" + (System.currentTimeMillis() - execTime));
            System.out.println("Tempo de Execução do SkyMap : " + (System.currentTimeMillis() - execTime));

            /*    try {
             //String params = "/home/hduser/teste.sh";
             long execTime = System.currentTimeMillis();
             String params = "/usr/bin/python /home/hduser/skymapadd/skymapadd.py "
             + " /home/hduser/QEF_Hadoop/Infos/pkls/"
             + " /home/hduser/FUNFOallsky.png";

             //ProcessBuilder pb = new ProcessBuilder(params);

             pr = Runtime.getRuntime().exec(params);

             String line=null;
             BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

             while((line=input.readLine()) != null) {
             System.out.println(line);
             }

             BufferedReader error = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
             while((line=error.readLine()) != null) {
             System.out.println("ERROR = "+line);
             }
             pr.waitFor();
             System.out.println("Tempo de Execução do SkyADD : " + (System.currentTimeMillis() - execTime));

             } catch(Exception e) {
             System.out.println(e.toString());
             e.printStackTrace();
             }*/
            return null;
        }
        limit++;
        //object = (DoubleType) instance.getData(0);
        //BD object = (OracleType) instance.getData(0);
        //BD fw.write(object.getObject().toString() + ":");
        //hdfs 
        object = (StringType) instance.getData(0);
        //hdfs 
        fw.write(object.toString() + ":");

        // object = (DoubleType) instance.getData(1);
        //BD object = (OracleType) instance.getData(1);
        //BD fw.write(object.getObject().toString() + ":");
        //hdfs 
        object = (StringType) instance.getData(1);
        //hdfs 
        fw.write(object.toString() + ":");
        return instance;
    }
}

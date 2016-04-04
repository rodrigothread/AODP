package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.nacluster.*;
import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import static ch.epfl.codimsd.qeef.Operator.logger;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class MapReduce extends Operator {

    private FileWriter fw;

    String numberOfPartition = null;
    String result = null;
    String Programjar = null;
    String configurationHdfs;
    String pathHadoop;
    //BD private OracleType object;
    Configuration conf;

    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public MapReduce(int id, OpNode opNode) throws Exception {
        super(id);
        
        pathHadoop = (String) (opNode.getParams()[0]);
        Programjar = (String) (opNode.getParams()[1]);

        //qefparameter=(String) (opNode.getParams()[0]);
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();
        System.out.println("OPEN MAPREDUCE");
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

        String path = "/tmp/" + hostname;
        fw = new FileWriter(path);

    }

    public void setMetadata(Metadata prdMetadata[]) {
        //this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        Instance instance = (Instance) super.getNext(id);
        long beginOpeningOperatorsTime = System.currentTimeMillis();
        BlackBoard bb = BlackBoard.getBlackBoard();
        String dataset = bb.get("hdfsCatalogo").toString();
        String partition = bb.get("hdfsPartition").toString();
        String appJar = bb.get("path").toString();
        numberOfPartition = (String) bb.get("nPartitions");
        appJar += "/app/OperatorMapRedFinal.jar";
        configurationHdfs = (String) bb.get("hdfsConf");
        conf = new Configuration();
        URI uri = new URI(configurationHdfs);
        FileSystem hdfs = FileSystem.get(uri, conf);
        //Print the home directory
        // System.out.println("Home folder -" +hdfs.getHomeDirectory());
        // Create
        Path workingDir = hdfs.getWorkingDirectory();
        String nomeArquivo = dataset.substring(dataset.lastIndexOf("/"), dataset.length() - 4);
        result = workingDir + "/cataloguesPartitioned" + nomeArquivo + "_" + numberOfPartition;
        //System.out.println("Map Reduce\n");
        //removed class name
        String[] command = {pathHadoop, "jar", appJar, dataset, partition, result, numberOfPartition};

        //System.out.println("appJar = " + appJar);
        //System.out.println("dataset = " + dataset);
        //System.out.println("partition = " + partition);
        //System.out.println("result = " + result);
        //System.out.println("numberofPartition = " + numberOfPartition);
        bb.put("catalogPartitioned", result);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = processBuilder.start();
        p.waitFor();
        logger.info("\nFINAL TIME EXECUTION "+MapReduce.class+" = "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - beginOpeningOperatorsTime) +" Segundos.\n");
        

        // System.exit(0);
        return null;
    }
}

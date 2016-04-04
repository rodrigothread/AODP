package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.Acesso;
import ch.epfl.codimsd.qeef.linea.*;
import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import static ch.epfl.codimsd.qeef.Operator.logger;
import ch.epfl.codimsd.qeef.relational.Tuple;
import ch.epfl.codimsd.qeef.types.DoubleType;
import ch.epfl.codimsd.qeef.types.FloatType;
import ch.epfl.codimsd.qeef.types.IntegerType;
import ch.epfl.codimsd.qeef.types.OracleType;
import ch.epfl.codimsd.qeef.types.StringType;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class France extends Operator {

    String numberOfPartitions = null;
    String javaParameters=null;
    
    //String file = null;
    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public France(int id, OpNode opNode) throws Exception {
        super(id);
        javaParameters =(String) (opNode.getParams()[0]);
        numberOfPartitions = (String) (opNode.getParams()[1]);
    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        System.out.println("OPEN FRANCE");
    }

    public void setMetadata(Metadata prdMetadata[]) {
        // this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        long beginOpeningOperatorsTime = System.currentTimeMillis();
        BlackBoard bb = BlackBoard.getBlackBoard();
        bb.put("nPartitions", numberOfPartitions);
        String fileOut = null;
        String franceExe = bb.get("path").toString();
        String fileIn = null;

        String dataset = bb.get("dataset").toString();
        String[] datasetSplitted = dataset.split("/");
        String datasetPath = dataset.substring(0, (dataset.length() - datasetSplitted[datasetSplitted.length - 1].length()));

        //file = (String) bb.get("dataset");
        fileOut = "Dataset/partitions/partitions_" + numberOfPartitions + "_" + datasetSplitted[datasetSplitted.length - 1];
        bb.put("partitionFile", fileOut);

        File pastaTemp = new File(datasetPath + "/partitions");
        //System.out.println("pastaTemp = " + pastaTemp.getAbsolutePath());
        if (!pastaTemp.exists()) {
            pastaTemp.mkdirs();
            //System.out.println(pastaTemp.getAbsoluteFile());
            //System.out.println(pastaTemp.exists());

        }

        franceExe += "/app/France.jar";

        fileIn = dataset;

         
        String comando =  "java "+"-jar "+javaParameters+" "+franceExe+" "+ dataset +" "+ numberOfPartitions;

        //String command[] = {franceExe, fileIn, numberOfPartitions};
        
        
        //String command[] = {franceExe, fileIn};

        //String[] command = {"spark-submit", "--verbose", "--conf","spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC","--conf", "spark.yarn.executor.memoryOverhead=512", "/home/dexl/dist/app/ParallelNACluster.jar", "hdfs://mnmaster:9000/user/dexllab/cataloguesPartitioned/2mass1MilhaoShaked3AmbNivel0_3.txt", "hdfs://mnmaster:9000/user/dexllab/ParallelNacluster/Resultado2mass1MilhaoShaked3AmbNivel0_3" ,"hdfs://mnmaster:9000/user/dexllab/partitions/partitions_3_2mass1MilhaoShaked3AmbNivel0.txt", "0", "0"};
        String[] command = comando.split(" ");

        System.out.println("command = " + comando);
        Process proc = Runtime.getRuntime().exec(command);

        //System.out.println(Arrays.toString(command));
        //System.out.println(proc.getOutputStream());
        File file = new File(fileOut);

        // if file doesnt exists, then create it
        if (!file.exists()) {
            //System.out.println("file out = " + file.getAbsolutePath());
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        // Read the output
        BufferedReader reader
                = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        String line = "";

        while ((line = reader.readLine()) != null) {
            bw.write(line + "\n");
            bw.flush();
        }

        bw.close();

        proc.waitFor();
        System.out.println("Arquivo partitions foi criado em "+ fileOut);

        
        //String[] command = comando.split(",");
        //String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit","--master","spark://master:7077",appJar+Programjar,"hdfs://mnmaster:9000/user/dexllab/catalogos/2mass1MilhaoShaked3AmbNivel0.txt","hdfs://mnmaster:9000/user/dexllab/ParallelNacluster/Resultado2mass1MilhaoShaked3AmbNivel0_3","hdfs://mnmaster:9000/user/dexllab/partitions/partitions_3_2mass1MilhaoShaked3AmbNivel0.txt"};
        //spark-submit --master spark://master:7077 /home/dexl/vini/ParallelNACluster.jar hdfs://mnmaster:9000/home/vini/saida hdfs://mnmaster:9000/home/vini/results /home/dexl/Project_QEF_1.0/data/partitions.txt 0 0
        // String[] command = {Spark, Programjar, dataset, result, partition};
        //String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit","--master","spark://master:7077",Programjar,"hdfs://146.134.232.10:9000/user/rodrigob/result","hdfs://mnmaster:9000/user/rodrigob/vini/results","/home/rodrigob/vini/catalogos/fronteiras.txt","0","0"};
        //command
       
        logger.info("\nFINAL TIME EXECUTION "+ France.class +" = "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - beginOpeningOperatorsTime) +" Segundos.\n");
        

        return null;

    }

}

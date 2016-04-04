package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import static ch.epfl.codimsd.qeef.Operator.logger;
import ch.epfl.codimsd.qep.OpNode;
import java.io.FileWriter;
import java.net.InetAddress;
import java.net.URI;
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
public class ParallelNACluster extends Operator {

    private FileWriter fw;
    Configuration conf;

    String Programjar = null;
    String result = null;
    String Spark = null;

    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public ParallelNACluster(int id, OpNode opNode) throws Exception {
        super(id);

        Spark = (String) (opNode.getParams()[0]);
        Programjar = (String) (opNode.getParams()[1]);

    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();

        System.out.println("OPEN PARALLEL");
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
        //String partitionFile = (String) bb.get("partitionFile");//obtém o arquivo partition do filesystem local
        String dataset = bb.get("catalogPartitioned").toString();
        String partition = bb.get("hdfsPartition").toString();//obtém do hdfs
        String appJar = bb.get("path").toString();//o caminho para as aplicações
        String configurationHdfs = (String) bb.get("hdfsConf");
        String numberOfPartition = (String) bb.get("nPartitions");

        //System.out.println("hdfsCatalogo Dataset = " + dataset);

        configurationHdfs = (String) bb.get("hdfsConf");
        conf = new Configuration();
        URI uri = new URI(configurationHdfs);
        FileSystem hdfs = FileSystem.get(uri, conf);
        //Print the home directory
        // System.out.println("Home folder -" +hdfs.getHomeDirectory());
        // Create

        Path workingDir = hdfs.getHomeDirectory();

        String nomeArquivo = dataset.substring(dataset.lastIndexOf("/") + 1, dataset.length());
        //System.out.println("nome do arquivo dataset = " + nomeArquivo);
        result = workingDir + "/ParallelNacluster/Resultado" + nomeArquivo;

        Path newFolderPath = new Path("/ParallelNacluster");

        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (!hdfs.exists(newFolderPath)) {

            hdfs.mkdirs(newFolderPath);

            //System.out.println("A pasta com o nome data foi criada " + newFolderPath);

        }

        /**
         * ", --master yarn-cluster --executor-memory 5G --executor-cores 2 " +
         * "--num-executors 4 --verbose --conf
         * \"spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC\" " +
         * "--conf \"spark.yarn.executor.memoryOverhead=512\"" +
         *
         *
         * spark-submit --master yarn-cluster --executor-memory 5G
         * --executor-cores 2 --num-executors 4 --verbose --conf
         * "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC" --conf
         * "spark.yarn.executor.memoryOverhead=512"
         * /home/dexl/dist/app/ParallelNACluster10.jar
         * hdfs://mnmaster:9000/user/dexllab/cataloguesPartitioned/2mass1MilhaoShaked3AmbNivel0_3.txt
         * hdfs://mnmaster:9000/user/dexllab/ParallelNacluster/Resultado2mass1MilhaoShaked3AmbNivel0_3
         * hdfs://mnmaster:9000/user/dexllab/partitions/partitions_3_2mass1MilhaoShaked3AmbNivel0.txt
         * 0 0
         *
         */
        
        Programjar = appJar.concat(Programjar);
        
        String comando =  Spark + " " + Programjar + " " + dataset + " " + result + " " + partition + " 0 0";

        //System.out.println("comando = " + comando);
        //System.out.println("y soyyy path == " + bb.get("path").toString());
        //System.out.println("Programjar = " + Programjar);

        //String[] command = {"spark-submit", "--verbose", "--conf","spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC","--conf", "spark.yarn.executor.memoryOverhead=512", "/home/dexl/dist/app/ParallelNACluster.jar", "hdfs://mnmaster:9000/user/dexllab/cataloguesPartitioned/2mass1MilhaoShaked3AmbNivel0_3.txt", "hdfs://mnmaster:9000/user/dexllab/ParallelNacluster/Resultado2mass1MilhaoShaked3AmbNivel0_3" ,"hdfs://mnmaster:9000/user/dexllab/partitions/partitions_3_2mass1MilhaoShaked3AmbNivel0.txt", "0", "0"};
        String[] command = comando.split(" ");

        //String[] command = comando.split(",");
        //String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit","--master","spark://master:7077",appJar+Programjar,"hdfs://mnmaster:9000/user/dexllab/catalogos/2mass1MilhaoShaked3AmbNivel0.txt","hdfs://mnmaster:9000/user/dexllab/ParallelNacluster/Resultado2mass1MilhaoShaked3AmbNivel0_3","hdfs://mnmaster:9000/user/dexllab/partitions/partitions_3_2mass1MilhaoShaked3AmbNivel0.txt"};
        //spark-submit --master spark://master:7077 /home/dexl/vini/ParallelNACluster.jar hdfs://mnmaster:9000/home/vini/saida hdfs://mnmaster:9000/home/vini/results /home/dexl/Project_QEF_1.0/data/partitions.txt 0 0
        // String[] command = {Spark, Programjar, dataset, result, partition};
        //String[] command = {"/usr/local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit","--master","spark://master:7077",Programjar,"hdfs://146.134.232.10:9000/user/rodrigob/result","hdfs://mnmaster:9000/user/rodrigob/vini/results","/home/rodrigob/vini/catalogos/fronteiras.txt","0","0"};
        //command
        
        ProcessBuilder processBuilder = new ProcessBuilder(command);

        processBuilder.redirectErrorStream(
                true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = processBuilder.start();

        p.waitFor();

        logger.info("\nFINAL TIME EXECUTION "+ParallelNACluster.class+" = "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - beginOpeningOperatorsTime) +" Segundos.");
        
      
        

        return null;
    }
}

package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataUnit;
import ch.epfl.codimsd.qeef.Instance;
import ch.epfl.codimsd.qeef.Metadata;
import ch.epfl.codimsd.qeef.Operator;
import ch.epfl.codimsd.qep.OpNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


/*
 * Esta classe é responsável por Identificar as tabelas que tenham tuplas modificadas
 * e também por montar o grafo de dependências das tabelas que serão copiadas. As tabelas
 * que devem ter seus dados copiados são inseridas no ArrayList e passadas para o operador
 * replicador.
 */
public class CopytoHdfs extends Operator {

    //private DoubleType object;
    //hdfs 
    String configurationHdfs;
    //BD private OracleType object;
    Configuration conf;
    String fileinput;
    String fileinput2;
    String filewrite1;
    String filewrite2;
    
    
    //==========================================================================================
    // Construtores e funcoes utilizadas por ele
    //==========================================================================================
    public CopytoHdfs(int id, OpNode opNode) throws Exception {
        super(id);

        configurationHdfs = (String) (opNode.getParams()[0]);

    }

    //=========================================================================================
    //Overrrided functions
    //=========================================================================================
    public void open() throws Exception {

        super.open();
        
        System.out.println("OPEN COPYTOHDFS");
        InetAddress addr = InetAddress.getLocalHost();
        String hostname = addr.getHostName();

    }

    public void setMetadata(Metadata prdMetadata[]) {
//                this.metadata[0] = (Metadata)prdMetadata[0].clone();
    }

    public void close() throws Exception {

        super.close();
    }

    public DataUnit getNext(int consumerId) throws Exception {

        Instance instance = (Instance) super.getNext(id);
        
        //logger.info("\n Starting CopytoHdfs "+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
        long beginOpeningOperatorsTime = System.currentTimeMillis();
        System.out.println("OPERADOR COPYTOHDFS INICIANDO");
        BlackBoard bb = BlackBoard.getBlackBoard();
        fileinput = bb.get("dataset").toString();
        fileinput2 = bb.get("partitionFile").toString();
        bb.put("hdfsConf", configurationHdfs);

       
        
        
        conf = new Configuration();
        URI uri = new URI(configurationHdfs);
        FileSystem hdfs = FileSystem.get(uri, conf);

        //Print the home directory
        // System.out.println("Home folder -" +hdfs.getHomeDirectory());
        // Create
        Path workingDir = hdfs.getWorkingDirectory();
        Path newFolderPath = new Path("/catalogos");
        Path newFolderPath2 = new Path("/partitions");
        newFolderPath = Path.mergePaths(workingDir, newFolderPath);
        newFolderPath2 = Path.mergePaths(workingDir, newFolderPath2);
        if (!hdfs.exists(newFolderPath)) {

            hdfs.mkdirs(newFolderPath);
            hdfs.mkdirs(newFolderPath2);//Create new Directory
            System.out.println("A pasta com o nome data foi criada " + hdfs.getHomeDirectory());

        }
        //}

        String data = fileinput.substring(fileinput.lastIndexOf("/"), fileinput.length());
        String data2 = fileinput2.substring(fileinput2.lastIndexOf("/"), fileinput2.length());

        //Copying File from local to HDFS
        Path localFilePath = new Path(fileinput);
        Path hdfsFilePath = new Path(newFolderPath + data);
        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

        System.out.println("O arquivo foi copiado para o HDFS. " + hdfsFilePath);

        Path localFilePath2 = new Path(fileinput2);
        Path hdfsFilePath2 = new Path(newFolderPath2 + data2);
        hdfs.copyFromLocalFile(localFilePath2, hdfsFilePath2);
        bb.put("hdfsCatalogo", hdfsFilePath);
        bb.put("hdfsPartition", hdfsFilePath2);
        //System.out.println("O arquivo foi copiado para o HDFS. " + hdfsFilePath2);
        //System.out.println("Operator CopytoHdfs foi terminado com sucesso.\n");
        logger.info("\nFINAL TIME EXECUTION "+CopytoHdfs.class+" = "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - beginOpeningOperatorsTime) +" Segundos.\n");
     
        return null;
    }

}

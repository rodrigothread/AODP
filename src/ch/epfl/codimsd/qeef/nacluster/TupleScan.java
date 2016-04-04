package ch.epfl.codimsd.qeef.nacluster;

import ch.epfl.codimsd.qeef.Acesso;
import ch.epfl.codimsd.qeef.BlackBoard;
import ch.epfl.codimsd.qeef.DataSource;
import ch.epfl.codimsd.qeef.DataSourceManager;
import ch.epfl.codimsd.qeef.DataUnit;
import static ch.epfl.codimsd.qeef.Operator.logger;
import ch.epfl.codimsd.qep.OpNode;
import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Realiza a leitura completa de uma fonte de dados retornando as instancias
 */
public class TupleScan extends Acesso {

    /**
     * Construtor padr�o.
     *
     * @param id Identificador deste operador.
     * @param dataSource Fonte de dados que ser� lida.
     */
    String file = null;
    long beginOpeningOperatorsTime = System.currentTimeMillis();
    
    public TupleScan(int id, OpNode op) {

        super(id);

        String caminho = "/" + getInstallPath(TupleScan.class);
        
        BlackBoard bb = BlackBoard.getBlackBoard();
        bb.put("dataset", caminho + op.getParams()[1]);
        bb.put("path", caminho);

        // Get the DataSource of this Scan operator.
        DataSourceManager dsManager = DataSourceManager.getDataSourceManager();
        dataSource = (DataSource) dsManager.getDataSource(op.getOpTimeStamp());

    }

    private static String findJarParentPath(File jarFile) {
        while (jarFile.getPath().contains(".jar")) {
            jarFile = jarFile.getParentFile();
        }
        return jarFile.getPath().substring(6);
    }

    /**
     * Return the instalation path of any class.
     *
     * @param theClass The class to find the installation path.
     * @return The installation path of any class.
     */
    public static String getInstallPath(Class< ?> theClass) {
        String url = theClass.getResource(theClass.getSimpleName() + ".class").getPath();
        File dir = new File(url).getParentFile();
        if (dir.getPath().contains(".jar")) {
            return findJarParentPath(dir).replace("%20", " ");
        }

        return dir.getPath().replace("%20", " ");
    }

    /**
     * Realiza a inicializa��o da fonte de dados utilizada e define o formato
     * das inst�ncias retornadas por este operador. O formato das inst�ncias
     * ser� o mesmo que os da fonte de dados.
     *
     * @throws java.io.IOException Se algum erro acontecer durante a
     * inicializa��o da fonte de dados.
     */
    public void open() throws Exception {
        //System.out.println("Scan open");
        super.open();
        System.out.println("OPEN TUPLESCAN");

    }

    /**
     * Retorna a pr�xima inst�ncia da fonte de dados.
     *
     * @param consumerId Identificador do consumidor.
     *
     * @throws Exception Se acontecer alguma problema durante a leitura da fonte
     * de dados.
     */
    public DataUnit getNext(int consumerId) throws Exception {
        long beginOpeningOperatorsTime = System.currentTimeMillis();
        instance = (dataSource).read();

        
        logger.info("\n FINAL TIME EXECUTION "+TupleScan.class +" = "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - beginOpeningOperatorsTime) +" Segundos.");
        
        return instance;
    }

    /**
     * Close the operator.
     */
    public void close() throws Exception {

        super.close();
    }
}

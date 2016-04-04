package ch.epfl.codimsd.qeef;

import ch.epfl.codimsd.exceptions.operator.OperatorInitializationException;
import java.util.Hashtable;

import java.util.logging.Level;
import org.apache.log4j.Logger;

import ch.epfl.codimsd.exceptions.dataSource.CatalogException;
import ch.epfl.codimsd.exceptions.initialization.InitializationException;
import ch.epfl.codimsd.exceptions.initialization.QEPInitializationException;
import ch.epfl.codimsd.exceptions.operator.OperatorException;
import ch.epfl.codimsd.exceptions.optimizer.OptimizerException;
import ch.epfl.codimsd.qeef.operator.OperatorFactoryManager;
import ch.epfl.codimsd.qeef.operator.modules.ModFix;
import ch.epfl.codimsd.qeef.util.Constants;
import ch.epfl.codimsd.qep.OpNode;
import ch.epfl.codimsd.qep.QEP;
import ch.epfl.codimsd.qep.QEPFactory;
import ch.epfl.codimsd.query.Request;

/**
 * Define uma implementa��o para o gerente de plano baseado em arquivo texto. Os
 * planos devem ter o seguinte formato:
 * <ol>
 * <li> N�mero de operadores no plano
 * <li> Defini��o de um operador. ID NOME;[parametro[;parametro]*]
 * <li> Numero de m�dulos(define tipo de link entre operadores).
 * <li> Nome Modulo;Operador;Produtor[;Produtor]
 * <ol>
 *
 * <b>OBS:</b> Os operadores definidos no plano devem ser reconhecidos pela
 * inst�ncia da fabrica de operadores utilizada.
 *
 * @author Vinicius Fontes
 */
public class WEBBPlanManager implements PlanManager {

    /**
     * Hashtable containing the operators.
     */
    private Hashtable<Integer, Operator> operatorHashtable;

    /**
     * The OperatorFactoryManager object used to create the operators.
     */
    private OperatorFactoryManager operatorFactoryManager;

    /**
     * Default constructor.
     *
     * @param operatorFactoryManager
     */
    public WEBBPlanManager(OperatorFactoryManager operatorFactoryManager) {

        this.operatorFactoryManager = operatorFactoryManager;
    }

    /**
     * Log4j logger
     */
    private static Logger logger = Logger.getLogger(WEBBPlanManager.class.getName());

    /**
     * Instancia o plano de execu��o deinido em planReader. Cria os operadores,
     * m�dulos e liga os consumidores/produtores.
     *
     * @param planReader Fluxo de leitura para o plano a ser instanciado.
     * @param dsManager Gerente de fonte de dados utilizado nesta inst�ncia da
     * m�quina de execu��o.
     *
     * @return Operadores do plano instanciados e interligados. Operadores est�o
     * indexados pelo seu id.
     *
     * @throws IOException Se acontecer algum problema durante a leitura do
     * plano.
     * @throws Exception Se acontecer algum problema durante a cria��o de um
     * operador.
     */
    public QEP instantiatePlan(String qepString) throws OperatorException, InitializationException, OperatorInitializationException {

        QEP qepInitial = null;

        try {
            qepInitial = new QEP();
            qepInitial.setOpList(QEPFactory.loadQEP(qepString, "", qepInitial));

        } catch (QEPInitializationException ex) {
            ex.printStackTrace();
            throw new InitializationException("Cannot load the remote QEP : " + ex.getMessage());
        }

        buildConcretePlan(qepInitial);
        qepInitial.setConcreteOpList(operatorHashtable);

        return qepInitial;
    }

    public QEP instantiatePlan(Request request) throws InitializationException, OptimizerException, OperatorException, OperatorInitializationException {

        // long codimsPlanConstructionTime = System.currentTimeMillis();
        QEP qepInitial = new QEP();
        qepInitial.existRemote = false;

        try {
            QEPFactory qepFactory = new QEPFactory();
            String qepInitialFile = qepFactory.getQEP(request.getRequestType(), 0);
            qepInitial.setOpList(QEPFactory.
                    loadQEP(qepInitialFile, Constants.qepAccessTypeLocal, qepInitial));

        } catch (CatalogException ex) {
            throw new InitializationException("InitializationException : " + ex.getMessage());
        }

        try {
            buildConcretePlan(qepInitial);
            qepInitial.setConcreteOpList(operatorHashtable);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(WEBBPlanManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return qepInitial;
    }

    private void buildConcretePlan(QEP qep) throws OperatorException, OperatorInitializationException {

        operatorHashtable = new Hashtable<Integer, Operator>();

        for (int i = 1; i <= qep.getOperatorList().size(); i++) {

            OpNode opNode = (OpNode) qep.getOperatorList().get(i + "");

            if (opNode != null) {
                Operator op = operatorFactoryManager.createOperator(opNode);
                operatorHashtable.put(new Integer(op.getId()), op);
            }
        }

        ModFix modFix = null;
        boolean crioumodfix = false;

        // create producer-consumer relations using ModFix class
        for (int i = 1; i <= qep.getOperatorList().size(); i++) {

            OpNode opNode = (OpNode) qep.getOperatorList().get(i + "");

            if (opNode != null) {

                Operator op = (Operator) operatorHashtable.get(new Integer(i));

                if (opNode.getProducerIDs()[0] != 0) {

                    int[] intProducers = opNode.getProducerIDs();

                    for (int j = 0; j < intProducers.length; j++) {

                        Operator producer = (Operator) operatorHashtable.get(new Integer(intProducers[j]));
                        if (!crioumodfix) {
                            modFix = new ModFix(op, producer);
                            crioumodfix = true;
                        } else {
                            modFix.adicionar(op, producer);
                        }
                    }
                }
            }
        }

        // logger.debug("codimsBuildingOpTime : " + (System.currentTimeMillis() - codimsBuildingOpTime));
    }
}

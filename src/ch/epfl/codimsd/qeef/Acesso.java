/*
 * CoDIMS version 1.0 
 * Copyright (C) 2006 Othman Tajmouati
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package ch.epfl.codimsd.qeef;

/**
 * Classe que generaliza operadores alg�bricos de acesso.
 *
 * Changes made by Othman : - constructor changed, we pass only the id (no more
 * dataSource and BlackBoard).
 *
 * @author Fausto Ayres, Vinicius Fontes, Othman Tajmouati.
 *
 * @date Jun 19, 2005
 */
public class Acesso extends Operator {

    /**
     * Fonte de Dados acessada por este operador de acesso. Source of Data
     * accessed by this operator of access
     */
    protected DataSource dataSource;

    /**
     * Construtor padr�o.
     *
     * @param id Identificador deste operador.
     * @param dataSource Fonte de dados a ser acessada.
     * @param Quadro de comunica��o utilizado pelos operadores de um plano de
     * execu��o.
     *
     */
    public Acesso(int id) {
        super(id);
    }

    /**
     * Inicializa fonte de dados utilizada. Initialize the source of data used.
     *
     * @throws Exception Se acontecer algum problem na inicializa��o da fonte de
     * dados.
     */
    public void open() throws Exception {
        dataSource.open();
        super.open();
    }

    /**
     * Define o formato dos dados roduzidos por este operador que � igual ao
     * formato da fonte de dados. It defines the format of the data roduzidos
     * for this operator who is equal to the format of the source of data.
     *
     * @param prdMetadata Deve ser null. Um operador de acesso s� acessa a fonte
     * de dados e portanto n�o possui produtores.
     */
    public void setMetadata(Metadata prdMetadata[]) {
        this.metadata = new Metadata[1];
//        this.metadata[0] = (Metadata)dataSource.getMetadata().clone();
    }

    /**
     * Finaliza/encerra fonte de dados.
     *
     * @throws Exception Se acontecer algum problema no encerramento da fonte de
     * dados.
     */
    public void close() throws Exception {
        super.close();
        dataSource.close();
    }

}

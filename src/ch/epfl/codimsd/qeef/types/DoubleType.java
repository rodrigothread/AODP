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
/*
 * Created on Mar 9, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ch.epfl.codimsd.qeef.types;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import ch.epfl.codimsd.qeef.Data;

/**
 *
 *
 *
 * @author Vinicius Fontes, Othman Tajmouati
 * @date Mar 9, 2005
 */
@SuppressWarnings("serial")
public class DoubleType implements Type {

    //Campos auxiliares utilizados na definicao e INTEGER_PATTERN
    public static char digSep, decSep;

    static {
        DecimalFormat df = new DecimalFormat();
        DecimalFormatSymbols dfs = df.getDecimalFormatSymbols();
        digSep = dfs.getGroupingSeparator();
        decSep = dfs.getDecimalSeparator();
    }

    /**
     * Define um n�mero fracion�rio e com ou sem formata��o. Aceita qualquer
     * n�mero com ou sem separador decimal/digito
     */ //([0-9]+([" + digSep + "][0-9]+)*)
    public static final String DOUBLE_PATTERN = "(([0-9]+([" + digSep + "][0-9]+)*)([" + decSep + "][0-9]+))";

    private double value;

    //------------------------------------------------------------
    public DoubleType() {
        super();
    }

    public DoubleType(String value) {
        this.setValue(value);
    }

    public DoubleType(byte value[], int offset) {
        this.setValue(value, offset);
    }

    public DoubleType(double value) {
        this.value = value;
    }

    //------------------------------------------------------------
    public void setValue(byte value[], int offset) {
    }

    public void setValue(String value) {
        this.value = Double.parseDouble(value);
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setValue(Object value) {
        this.value = ((Double) value).doubleValue();
    }

    //------------------------------------------------------------
    public int compareTo(Object o) {

        return (int) (value - ((DoubleType) o).doubleValue());
    }

    public double doubleValue() {
        return value;
    }

    public Object clone() {
        DoubleType v = new DoubleType();
        v.value = this.value;
        return v;
    }

    public String toString() {
        return new String("" + value);
    }

    /*
     *
     */
    public void display(Writer out) throws IOException {
        out.write(value + " ");
    }

    /*
     *
     */
    public int displayWidth() {
        return 12;
    }

    /*
     *
     */
    public Type newInstance() {
        return new DoubleType();
    }


    /*
     *
     */
    public Type read(DataInputStream in) throws IOException {
        return new DoubleType(in.readDouble());
    }

    /*
     *
     */
    public void write(DataOutputStream out) throws IOException {
        out.writeDouble(value);
    }

    /**
     * A implementa��o deste m�todo default da linguagem se tornou obrigat�ria
     * com o objetivo de agilizar o processo de libera��o de mem�ria que �
     * realizado no Garbage Colector. Recomenda-se atribuir null a todas as suas
     * refer�ncias.
     */
    public void finalize() throws Throwable {
        super.finalize();
    }

    /**
     * @see Type.recognitionPattern()
     */
    public String recognitionPattern() {

        return DOUBLE_PATTERN;
    }

    public void setMetadata(Data data) {
    }
}

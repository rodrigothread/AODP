/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.codimsd.qeef.types;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import org.apache.log4j.Logger;

import ch.epfl.codimsd.qeef.Data;
import java.util.ArrayList;

/**
 * Classe especializada de Type que define o tipo String na m�quina de execu��o.
 *
 * @author Douglas
 *
 * @date Fev 15, 2012
 */
@SuppressWarnings("serial")
public class ArrayListType implements Type, Serializable {

    /**
     * Log4j logger.
     */
    @SuppressWarnings("unused")
    private static Logger logger = Logger.getLogger(StringType.class.getName());

    /**
     * Define uma String. Aceita qualquer sequ�ncia de caracter que come�e com "
     * termine com " e n�o contenha " no meio
     */
    public static final String STRING_PATTERN = "([\"][\\p{ASCII}][^\"]*[\"])";

    private ArrayList<String> value;

    //------------------------------------------------------------
    /**
     * Construtor pdr�o.
     */
    public ArrayListType() {
        super();
    }

    public ArrayListType(ArrayList<String> value) {
        this.value = value;
    }

    public void setValue(ArrayList<String> value) {

        this.value = value;
    }

    public void setValue(Object value) {
        setValue((ArrayList<String>) value);
    }

    public void setValue(String value) {
        this.value.add(value);
    }

    public ArrayList<String> getObject() {
        return value;
    }

    public int compareTo(Object o) {

        //return value.compareTo(o);
        return 0;
    }

    public Object clone() {
        ArrayListType v = new ArrayListType();
        v.value = new ArrayList<String>(this.value);
        return v;
    }

    /*
     *
     */
    public void display(Writer out) throws IOException {
        //out.add(value);
        //out.write(" ");
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
        return new ArrayListType();
    }


    /*
     *
     */
    public Type read(DataInputStream in) throws IOException {

        return null;
    }

    /*
     *
     */
    public void write(DataOutputStream out) throws IOException {

        //out.writeUTF(value);
    }

    /**
     * @see Type#recognitionPattern()
     */
    public String recognitionPattern() {

        return STRING_PATTERN;
    }

    /**
     * A implementa��o deste m�todo default da linguagem se tornou obrigat�ria
     * com o objetivo de agilizar o processo de libera��o de mem�ria que �
     * realizado no Garbage Colector. Recomenda-se atribuir null a todas as suas
     * refer�ncias.
     */
    public void finalize() throws Throwable {
        super.finalize();
        value = null;
    }

    public void setMetadata(Data data) {
    }

}

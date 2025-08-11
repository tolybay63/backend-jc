package tofi.apinator;

import jandcode.commons.named.*;

import java.lang.reflect.*;

/**
 * Описание параметра метода
 */
public interface ApinatorMethodParam extends INamed {

    /**
     * java параметр
     */
    Parameter getParameter();

    /**
     * Тип параметра
     */
    Class<?> getType();

    /**
     * Порядковый номер параметра
     */
    int getIndex();

}

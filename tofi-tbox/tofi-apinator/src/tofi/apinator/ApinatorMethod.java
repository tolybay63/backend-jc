package tofi.apinator;

import jandcode.commons.named.*;

import java.lang.reflect.*;

/**
 * Описание метода
 */
public interface ApinatorMethod extends INamed {

    /**
     * Ссылка на реальный java-метод
     */
    Method getMethod();

    /**
     * Параметры метода
     */
    NamedList<ApinatorMethodParam> getParams();

}

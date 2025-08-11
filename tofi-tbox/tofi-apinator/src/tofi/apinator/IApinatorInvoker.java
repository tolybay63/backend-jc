package tofi.apinator;

import jandcode.commons.conf.*;

import java.lang.reflect.*;

/**
 * Исполнитель конкретного метода api
 */
public interface IApinatorInvoker {

    /**
     * Выполнить метод
     *
     * @param cls         для какого класса
     * @param method      какой метод
     * @param contextConf конфигурация для контекста
     * @param args        аргументы
     * @return результат
     */
    Object invokeApi(Class cls, Method method, Conf contextConf, Object... args) throws Exception;

}

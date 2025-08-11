package tofi.apinator;

import jandcode.commons.conf.*;
import jandcode.core.*;

import java.lang.reflect.*;

/**
 * Контекст исполнения api
 */
public interface ApinatorContext extends IAppLink, BeanFactoryOwner, IConfLink {

    /**
     * Экземпляр класса, чей метод выполняется
     */
    Object getInst();

    /**
     * Какой метод исполняем
     */
    Method getMethod();

    /**
     * Кто исполняет
     */
    ApinatorInvoker getInvoker();

    /**
     * Время начала выполнения.
     */
    long getStartTime();

    /**
     * Результат выполнения метода, если метод удачно выполнился
     */
    Object getResult();

    /**
     * Пойманная ошибка, если метод закончил выполнение с ошибкой
     */
    Throwable getException();

    /**
     * Возвращает true, если выполнение прервалось с ошибкой.
     * {@link ApinatorContext#getException()} содержит ошибку.
     */
    boolean hasError();

}

package tofi.apinator;

/**
 * Логгер для api.
 */
public interface ApinatorLogger {

    /**
     * Превратить контекст в строку лога
     */
    String toString(ApinatorContext ctx);

    /**
     * Начало выполнения метода
     */
    void logStart(ApinatorContext ctx);

    /**
     * Конец выполнения метода
     */
    void logStop(ApinatorContext ctx);

}

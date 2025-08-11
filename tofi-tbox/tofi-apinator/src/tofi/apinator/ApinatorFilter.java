package tofi.apinator;

/**
 * Фильтр для процесса исполнения dao
 */
public interface ApinatorFilter {

    /**
     * Вызывается для каждого типа фильтра по необходимости
     *
     * @param type тип фильтра
     * @param ctx  контекст исполнения
     */
    void execApinatorFilter(ApinatorFilterType type, ApinatorContext ctx) throws Exception;

}

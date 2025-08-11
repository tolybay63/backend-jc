package tofi.mdl.model.dao.dimperiod.helpers;

import java.util.List;

/**
 * Используется при формировании сторе для исключаемых элементов узла периода
 */
public interface IPeriodItem {

    /**
     * Возвращает id типа периода
     *
     * @param link ссылка на узел периода
     * @return long
     */
    long getPeriodType(String link) throws Exception;

    /**
     * Возвращает список исключаемых элементов
     *
     * @param link ссылка на узел периода
     * @return List
     */
    List<Long> getNotInList(String link) throws Exception;

    /**
     * Вовращает ссылку на родителя узла периода
     *
     * @param link ссылка на узел периода
     * @return String
     */
    String getParentLink(String link) throws Exception;

}

package tofi.mdl.model.dao.dimperiod.helpers;


import jandcode.core.store.StoreRecord;

import java.util.List;

/**
 * Используется при формировании сторе для исключаемых элементов узла периода
 */
public interface IPeriodElement {


    /**
     * Возвращает список записей подуровней переданного id уровня
     *
     * @param elementLink ссылка на родительский ЭЛЕМЕНТ периода, который содержит внутри id УРОВНЯ периода
     * @return List
     */
    List<StoreRecord> getItemChildRec(String elementLink) throws Exception;

    /**
     * Возвращает запись уровня периода
     *
     * @param itemLink ссылка на УРОВЕНЬ периода
     * @return StoreRecord
     */
    StoreRecord getItemRec(String itemLink) throws Exception;

    /**
     * Возвращает список исключаемых элементов
     *
     * @param itemLink ссылка на УРОВЕНЬ периода
     * @return List
     */
    List<Long> getNotInList(String itemLink) throws Exception;


}

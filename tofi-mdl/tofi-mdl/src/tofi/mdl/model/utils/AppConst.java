package tofi.mdl.model.utils;

import jandcode.commons.UtDateTime;
import jandcode.commons.datetime.XDate;

/**
 * Различные глобальные константы
 */
public class AppConst {
    /**
     * Специальная дата, которая рассматривается как пустая дата начала периода
     */
    public static XDate DBEG_EMPTY = XDate.create("1800-01-01");

    /**
     * Специальная дата, которая рассматривается как пустая дата конца периода
     */
    public static XDate DEND_EMPTY = XDate.create("3333-12-31");


    /**
     * Проверка, что дата пустая
     */
    public static boolean isDateEmpty(XDate d) {
        return UtDateTime.isEmpty(d) || d.equals(DBEG_EMPTY);
    }

    /**
     * Формат короткой даты без времени
     */
    public static String DATE_SHORT = "yyyy-MM-dd";

    /**
     * Язык поумалчанию
     */
    public static long DEFAULT_LANG = 1;
}

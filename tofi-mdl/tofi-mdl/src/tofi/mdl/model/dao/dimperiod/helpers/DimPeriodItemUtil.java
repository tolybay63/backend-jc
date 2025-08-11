package tofi.mdl.model.dao.dimperiod.helpers;

import jandcode.commons.UtLang;
import jandcode.commons.datetime.XDate;
import jandcode.commons.error.XError;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_PeriodType_consts;
import tofi.mdl.model.utils.AppConst;
import tofi.mdl.model.utils.period.UtPeriod;

public class DimPeriodItemUtil {

    /**
     * Возвращает dend
     * если dend не указано, вычисляет конец периода по полям periodType и lagCurrentDate (относительно today)
     */
    public XDate getDend(StoreRecord rec, long periodType, XDate currDate) {

        if (!AppConst.isDateEmpty(rec.getDate("dend"))) {
            return rec.getDate("dend");
        }

        if (rec.isValueNull("lagCurrentDate")) {
            throw new XError(UtLang.t("Конец периода не назначен"), "dend");
            //return null
        }

        XDate res;
        UtPeriod pu = new UtPeriod();
        res = pu.calcDend(currDate, periodType, rec.getInt("lagCurrentDate"));
        return res;
    }


    /**
     * Возвращает dbeg
     * если dbeg не указано, вычисляет начало периода по полям periodType, countPeriod и концу периода
     */
    public XDate getDbeg(StoreRecord rec, long periodType, XDate currDate) {

        if (!AppConst.isDateEmpty(rec.getDate("dbeg"))) {
            return rec.getDate("dbeg");
        }

        if (rec.getInt("countPeriod") == 0) {
            throw new XError(UtLang.t("Начало периода не назначен"), "dbeg");
        }

        if (rec.getInt("countPeriod") < 0) {
            throw new XError(UtLang.t("Поле {0} не может иметь отрицательное число", "countPeriod"), "countPeriod");
        }

        int countPeriod = rec.getInt("countPeriod");

        XDate dend = getDend(rec, periodType, currDate);

        //ToDo
        if (periodType == FD_PeriodType_consts.halfyearaccum
                || periodType == FD_PeriodType_consts.quarteraccum
                || periodType == FD_PeriodType_consts.monthaccum
                || periodType == FD_PeriodType_consts.decadeaccum
                || periodType == FD_PeriodType_consts.weekaccum
                || periodType == FD_PeriodType_consts.nonstandard) {
            //адаптируем количество периодов
            int o = 0;
            //countPeriod = adopAccCountPeriod(countPeriod, periodType, dend);
        }

        if (periodType == FD_PeriodType_consts.week
                && !rec.isValueNull("includeTag")) {
            //адаптируем количество периодов

            long includeTag = rec.getLong("includeTag");

            if ((includeTag == 2 || includeTag == 3)
                    && dend.toJavaLocalDate().getDayOfWeek().getValue() != 7) {
                //&& dend.getDayOfWeek() != 7) {
                countPeriod++;
            }

            if (includeTag == 4 && dend.toJavaLocalDate().getDayOfWeek().getValue() < 4) {
                countPeriod++;
            }
        }

        /*так как countPeriod задается как положительное число,
         * его адаптируем для использования в PeriodUtils.calcDbeg()
         */
        countPeriod = (countPeriod - 1) * (-1);

        UtPeriod pu = new UtPeriod();

        return pu.calcDbeg(dend, periodType, countPeriod);
    }


}

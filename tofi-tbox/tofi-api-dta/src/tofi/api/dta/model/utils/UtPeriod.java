package tofi.api.dta.model.utils;

import jandcode.commons.datetime.*;
import jandcode.commons.error.*;
import tofi.api.mdl.model.consts.*;



import java.time.*;

public class UtPeriod {

    /**
     * Возвращает dbeg
     * если dbeg не указано, вычисляет начало периода по полям periodType, countPeriod и концу периода
     */
    public XDate calcDbeg(XDate date, long periodType, int countPeriod) {
        XDate res;
        if (periodType == FD_PeriodType_consts.month) {
            res = calcDbegMonth(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.year) {
            res = calcDbegYear(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.halfyear) {
            res = calcDbegHalfYear(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.quarter) {
            res = calcDbegQuarter(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.decade) {
            res = calcDbegDecada(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.week) {
            res = calcDbegWeek(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.day) {
            res = calcDbegDay(countPeriod, date);
        } else {
            throw new XError("Unknown periodType");
        }

        return res;
    }

    // вычисляет dbeg по periodCount, когда periodCount количество месяцев
    private XDate calcDbegMonth(int countPeriod, XDate dend) {
        return dend.addMonths(countPeriod).beginOfMonth();
    }

    // вычисляет dbeg по periodCount, когда periodCount количество годов
    private XDate calcDbegYear(int countPeriod, XDate dend) {
        return dend.addYears(countPeriod).beginOfYear();
    }

    // вычисляет dbeg по periodCount, когда periodCount количество полугодии
    private XDate calcDbegHalfYear(int countPeriod, XDate dend) {

        XDate curDbeg;
        if (dend.decode().getMonth() <= 6) {
            curDbeg = XDate.create (dend.decode().getYear() + "-01-01");
        } else {
            curDbeg = XDate.create (dend.decode().getYear() + "-07-01");
        }

        return curDbeg.addMonths(countPeriod * 6);
    }

    // вычисляет dbeg по periodCount, когда periodCount количество квартал
    private XDate calcDbegQuarter(int countPeriod, XDate dend) {

        XDate curDbeg;

        if (dend.decode().getMonth() <= 3) {
            curDbeg = XDate.create(dend.decode().getYear() + "-01-01");
        } else if (dend.decode().getMonth() <= 6) {
            curDbeg = XDate.create(dend.decode().getYear() + "-04-01");
        } else if (dend.decode().getMonth() <= 9) {
            curDbeg = XDate.create(dend.decode().getYear() + "-07-01");
        } else {
            curDbeg = XDate.create(dend.decode().getYear() + "-10-01");
        }
        return curDbeg.addMonths(countPeriod * 3);
    }

    // вычисляет dbeg по periodCount, когда periodCount количество декад
    private XDate calcDbegDecada(int countPeriod, XDate date) {
        int curTenDay;
        if (date.decode().getDay() <= 10) {
            curTenDay = 1;
        } else if (date.decode().getDay() <= 20) {
            curTenDay = 2;
        } else {
            curTenDay = 3;
        }
        return date.beginOfMonth().addDays(countPeriod*10).addDays(10*(curTenDay-1));
    }





    // вычисляет dbeg по periodCount, когда periodCount количество дней
    private XDate calcDbegDay(int countPeriod, XDate dend) {
        return dend.addDays(countPeriod);
    }

    /**
     * Возвращает корректный dend для PeriodType по заданой дате, и количеству периодов
     *
     * @param date точка отсчета
     * @param periodType тип периода
     * @param countPeriod количество периода (если 0 текущяя для date конец периода,
     * если >0 к текущему периоду добавляются периоды, если <0 минусуются
     * @return название периода
     */
    public XDate calcDend(XDate date, long periodType, int countPeriod) {
        XDate res;
        if (periodType == FD_PeriodType_consts.month) {
            res = calcDendMonth(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.year) {
            res = calcDendYear(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.halfyear) {
            res = calcDendHalfYear(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.quarter) {
            res = calcDendQuarter(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.decade) {
            res = calcDendDecada(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.week) {
            res = calcDendWeek(countPeriod, date);
        } else if (periodType == FD_PeriodType_consts.day) {
            res = calcDendDay(countPeriod, date);
        } else {
            throw new XError("Unknown periodType");
        }

        return res;
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество месяцев
    private XDate calcDendMonth(int lagCurrentDate, XDate date) {
        return date.addMonths(lagCurrentDate).endOfMonth();
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество годов
    private XDate calcDendYear(int lagCurrentDate, XDate date) {
        return date.addYears (lagCurrentDate).endOfYear();
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество полугодии
    private XDate calcDendHalfYear(int lagCurrentDate, XDate date) {

        XDate res = date;
        XDate curDend;

        if (res.decode().getMonth() <= 6) {
            curDend = XDate.create(res.decode().getYear() + "-06-30");
        } else {
            curDend = XDate.create(res.decode().getYear() + "-12-31");
        }

        res = curDend.addMonths(lagCurrentDate * 6).endOfMonth();

        return res;
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество квартал
    private XDate calcDendQuarter(int lagCurrentDate, XDate date) {

        XDate res = date;
        XDate curQuarterEnd;
        int plusMonth;

        if (res.decode().getMonth() <= 3) {
            plusMonth = 3 - res.decode().getMonth();
        } else if (res.decode().getMonth() <= 6) {
            plusMonth = 6 - res.decode().getMonth();
        } else if (res.decode().getMonth() <= 9) {
            plusMonth = 9 - res.decode().getMonth();
        } else {
            plusMonth = 12 - res.decode().getMonth();
        }
        curQuarterEnd = res.addMonths(plusMonth).endOfMonth();
        res = curQuarterEnd.addMonths(lagCurrentDate * 3).endOfMonth();

        return res;
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество декад
    private XDate calcDendDecada(int lagCurrentDate, XDate date) {
        int curTenDay;
        if (date.decode().getDay() <= 10) {
            curTenDay = 1;
        } else if (date.decode().getDay() <= 20) {
            curTenDay = 2;
        } else {
            curTenDay = 3;
        }
        return date.beginOfMonth().addDays(lagCurrentDate*10).addDays(10*(curTenDay-1)).addDays(9);

    }

    // вычисляет dbeg по lagCurrentDate, когда lagCurrentDate количество недель
    private XDate calcDbegWeek(int lagCurrentDate, XDate date) {
        int curWeekDay;
        if (date.decode().getDay() <= 7) {
            curWeekDay = 1;
        } else if (date.decode().getDay() <= 14) {
            curWeekDay = 2;
        } else if (date.decode().getDay() <= 21) {
            curWeekDay = 3;
        } else {
            curWeekDay = 4;
        }
        XDate d0 = date.beginOfMonth().addDays(lagCurrentDate*7);
        while (getDayNumberWeek(d0) != 1) {
            d0 = d0.addDays(-1);
        }
        if (date.toJavaLocalDate().getDayOfYear()-d0.toJavaLocalDate().getDayOfYear() < 7)
            return d0;
        else
            return d0.addDays(curWeekDay*7);
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество недель
    private XDate calcDendWeek(int lagCurrentDate, XDate date) {
        int curWeekDay;
        if (date.decode().getDay() <= 7) {
            curWeekDay = 1;
        } else if (date.decode().getDay() <= 14) {
            curWeekDay = 2;
        } else if (date.decode().getDay() <= 21) {
            curWeekDay = 3;
        } else {
            curWeekDay = 4;
        }
        XDate d0 = date.beginOfMonth().addDays(lagCurrentDate*7);
        while (getDayNumberWeek(d0) != 1) {
            d0 = d0.addDays(-1);
        }
        if (date.toJavaLocalDate().getDayOfYear()-d0.toJavaLocalDate().getDayOfYear() < 7)
            return d0.addDays(6);
        else
            return d0.addDays(curWeekDay*7).addDays(6);
    }


    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество дней
    private XDate calcDendDay(int lagCurrentDate, XDate date) {
        return date.addDays(lagCurrentDate);
    }

    protected static int getDayNumberWeek(XDate date) {
        DayOfWeek day = date.toJavaLocalDate().getDayOfWeek();
        return day.getValue();
    }

}

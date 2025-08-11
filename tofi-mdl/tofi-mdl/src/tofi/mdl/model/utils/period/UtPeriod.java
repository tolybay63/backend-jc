package tofi.mdl.model.utils.period;

import jandcode.commons.UtString;
import jandcode.commons.datetime.XDate;
import jandcode.commons.error.XError;
import tofi.mdl.consts.FD_PeriodType_consts;

import java.time.DayOfWeek;

public class UtPeriod {


    /**
     * Возвращает номер декады в году
     */
    public int getTenDayOfYear(XDate date) {
        int curTenDay;
        if (date.decode().getDay() <= 10) {
            curTenDay = 1;
        } else if (date.decode().getDay() <= 20) {
            curTenDay = 2;
        } else {
            curTenDay = 3;
        }
        return curTenDay;
    }

    /**
     * Возвращает номер полугодия в году
     */
    public int getHalfYearOfYear(XDate date) {
        int halfYearNo;
        if (date.decode().getMonth() <= 6) {
            halfYearNo = 1;
        } else {
            halfYearNo = 2;
        }
        return halfYearNo;
    }

    /**
     * Возвращает номер квартала в году
     */
    public int getQuarterOfYear(XDate date) {
        int quaterNo;
        if (date.decode().getMonth() <= 3) {
            quaterNo = 1;
        } else if (date.decode().getMonth() <= 6) {
            quaterNo = 2;
        } else if (date.decode().getMonth() <= 9) {
            quaterNo = 3;
        } else {
            quaterNo = 4;
        }
        return quaterNo;
    }

    /**
     * Возвращает номер недели в году
     */
    public int getNumWeekOfYear(XDate date) {
        XDate d1Y = date.beginOfYear();
        while (d1Y.decode().getDow() != 1) {
            d1Y = d1Y.addDays(-1);
        }
        while (date.decode().getDow() != 1) {
            date = date.addDays(-1);
        }
        return Math.abs(date.diffDays(d1Y)) / 7;// + 1;
    }


    /**
     * Возвращает номер семестра в учебном году
     */
    public int getSemesterOfAcademicYear(XDate date) {
        int semesterNo;
        if (date.decode().getMonth() >= 9 || date.decode().getMonth() == 1) {
            semesterNo = 1;
        } else {
            semesterNo = 2;
        }
        return semesterNo;
    }

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

    // вычисляет dbeg по countPeriod, когда countPeriod количество месяцев
    private XDate calcDbegMonth(int countPeriod, XDate dend) {
        return dend.addMonths(countPeriod).beginOfMonth();
    }

    // вычисляет dbeg по countPeriod, когда countPeriod количество годов
    private XDate calcDbegYear(int countPeriod, XDate dend) {
        return dend.addYears(countPeriod).beginOfYear();
    }

    // вычисляет dbeg по countPeriod, когда countPeriod количество полугодии
    private XDate calcDbegHalfYear(int countPeriod, XDate dend) {
        XDate curDbeg;
        if (dend.decode().getMonth() <= 6) {
            curDbeg = XDate.create(dend.decode().getYear() + "-01-01");
        } else {
            curDbeg = XDate.create(dend.decode().getYear() + "-07-01");
        }
        return curDbeg.addMonths(countPeriod * 6);
    }

    // вычисляет dbeg по countPeriod, когда countPeriod количество квартал
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

    // вычисляет dbeg по countPeriod, когда countPeriod количество дней
    private XDate calcDbegDay(int countPeriod, XDate dend) {
        return dend.addDays(countPeriod);
    }

    /**
     * Возвращает корректный dend для PeriodType по заданой дате, и количеству периодов
     *
     * @param date        точка отсчета
     * @param periodType  тип периода
     * @param countPeriod количество периода (если 0 текущяя для date конец периода,
     *                    если >0 к текущему периоду добавляются периоды, если <0 минусуются
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
        return date.addYears(lagCurrentDate).endOfYear();
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

    private int noDecade(XDate date) {
        int noDecade;
        if (date.decode().getDay() <= 10) {
            noDecade = 1;
        } else if (date.decode().getDay() <= 20) {
            noDecade = 2;
        } else {
            noDecade = 3;
        }
        return noDecade;
    }

    // вычисляет dbeg по countPeriod, когда countPeriod количество декад
    private XDate calcDbegDecada(int lagCurrentDate, XDate date) {
        int curTenDay = noDecade(date);
        int t = curTenDay + lagCurrentDate;
        int months = 0;
        if (t > 3) {
            months = t / 3;
            if ((t % 3) == 0) {
                months--;
            }
            t = t - months * 3;
        }
        if (t < 1) {
            months = Math.abs(t / 3) + 1;
            t = Math.abs(t + months * 3);
            months *= -1;
        }

        return XDate.create(date.addMonths(months).toString().substring(0, 8) +
                UtString.padLeft(String.valueOf(t * 10 - 9), 2, "0"));
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество декад
    private XDate calcDendDecada(int lagCurrentDate, XDate date) {
        XDate res = calcDbegDecada(lagCurrentDate, date);
        int noDecade = noDecade(res);
        if (noDecade < 3) {
            return res.beginOfMonth().addDays(10 * (noDecade - 1)).addDays(9);
        } else {
            return res.endOfMonth();
        }
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
        XDate d0 = date.addDays(lagCurrentDate * 7);

        while (d0.decode().getDow() != 1) {
            d0 = d0.addDays(-1);
        }
        return d0;
    }

    // вычисляет dend по lagCurrentDate, когда lagCurrentDate количество недель
    private XDate calcDendWeek(int lagCurrentDate, XDate date) {

        XDate d0 = calcDbegWeek(lagCurrentDate, date);
        return d0.addDays(6);
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

package tofi.api.mdl.utils.dimPeriod;

import jandcode.commons.UtDateTime;
import jandcode.commons.UtLang;
import jandcode.commons.datetime.XDate;
import jandcode.commons.error.XError;
import tofi.api.mdl.utils.UtPeriod;
import tofi.api.mdl.model.consts.*;

import java.util.ArrayList;
import java.util.List;

public class PeriodGenerator {

    public static final long simpleFull = 1;
    public static final long simpleShort = 2;
    public static final long arabicFull = 3;
    public static final long arabicShort = 4;
    public static final long romanFull = 5;
    public static final long romanShort = 6;
    public static final long hiddenParentFull = 7;


    public List<TofiPeriod> genPeriods(XDate dbeg, XDate dend, long periodType, long includeTag, long pattern) {

        if (includeTag == 4 && (periodType != FD_PeriodType_consts.week
                && periodType != FD_PeriodType_consts.weekaccum)) {
            throw new XError(UtLang.t("Заданные тег включения периода и тип периода не совпадают"));
        }

        if (periodType == FD_PeriodType_consts.halfyearaccum
                || periodType == FD_PeriodType_consts.quarteraccum
                || periodType == FD_PeriodType_consts.monthaccum
                || periodType == FD_PeriodType_consts.decadeaccum
                || periodType == FD_PeriodType_consts.weekaccum
                || periodType == FD_PeriodType_consts.dayaccum) {
            return genPeriodsA(dbeg, dend, periodType, includeTag, pattern);
        } else {
            return genPeriodsNormal(dbeg, dend, periodType, includeTag, pattern);
        }
    }

    private List<TofiPeriod> genPeriodsNormal(XDate dbeg, XDate dend, long periodType, long includeTag, long pattern) {

        List<TofiPeriod> periods = new ArrayList<TofiPeriod>();

        UtPeriod pu = new UtPeriod();
        XDate date1 = pu.calcDbeg(dbeg, periodType, 0);

        if (nextDbeg(dbeg, date1, periodType, includeTag)) {
            date1 = pu.calcDbeg(dbeg, periodType, 1);
        }

        XDate date2 = pu.calcDend(date1, periodType, 0);


        while (!stop(dend, date1, date2, periodType, includeTag)) {

            TofiPeriod period = new TofiPeriod();
            period.setDbeg(date1);
            period.setDend(date2);
            period.setName(getPeriodName(date1, date2, periodType, pattern));
            period.setPeriodType(periodType);

            periods.add(period);

            date1 = pu.calcDbeg(date1, periodType, 1);
            date2 = pu.calcDend(date2, periodType, 1);
        }

        return periods;
    }

    private List<TofiPeriod> genPeriodsA(XDate dbeg, XDate dend, long periodType, long includeTag, long pattern) {

        List<TofiPeriod> periods = new ArrayList<TofiPeriod>();

        //ToDo Not Relised

        return periods;
    }

    private boolean nextDbeg(XDate dbeg, XDate date, long periodType, long includeTag) {

        boolean res = false;

        if (date.toJavaLocalDate().isBefore(dbeg.toJavaLocalDate())) {

            if (periodType == FD_PeriodType_consts.week
                    && includeTag == 4) {
                if (dbeg.decode().getDow() > 3) {
                    res = true;
                }
            } else if (periodType == FD_PeriodType_consts.weekaccum
                    && includeTag == 4) {
                //Надо проверить!
                if (dbeg.beginOfYear().toJavaLocalDate().isEqual(dbeg.toJavaLocalDate()) &&
                        dbeg.decode().getDow() > 3) {
                    res = true;
                }

                if (!dbeg.beginOfYear().toJavaLocalDate().isEqual(dbeg.toJavaLocalDate())) {
                    res = true;
                }

            } else if (includeTag == 1 || includeTag == 3) {
                res = true;
            }

        }

        return res;
    }

    private boolean stop(XDate dend, XDate date1, XDate date2, long periodType, long includeTag) {

        boolean res = false;

        if (date1.toJavaLocalDate().isAfter(dend.toJavaLocalDate())) {
            res = true;
        } else if (date2.toJavaLocalDate().isAfter(dend.toJavaLocalDate())) {

            if ((periodType == FD_PeriodType_consts.week
                    || periodType == FD_PeriodType_consts.weekaccum) && includeTag == 4) {
                if (dend.toJavaLocalDate().getDayOfWeek().getValue() < 4) {
                    res = true;
                } else {
                    if (Math.abs(date2.diffDays(dend)) > 6) {
                        res = true;
                    }
                }
            } else if (includeTag == 2 || includeTag == 3) {
                res = true;
            }
        }

        if (date1.toJavaLocalDate().isAfter(date2.toJavaLocalDate())) {
            res = true;
        }

        return res;
    }


    /**
     * По дате начала, дате конца, типу периода и шаблону формирует его наименование
     *
     * @param dbeg       начало
     * @param dend       конец
     * @param periodType тип периода
     * @param pattern    id шаблона
     * @return назнвание периода
     */
    public String getPeriodName(XDate dbeg, XDate dend, long periodType, long pattern) {
        String r_text = getTextCustom(dbeg, dend);

        if (pattern == romanFull) {
            // цифры римские, текст полный
            r_text = getTextRomanFull(dbeg, dend, periodType);
        } else if (pattern == romanShort) {
            // цифры римские, текст короткий
            r_text = getTextRomanShort(dbeg, dend, periodType);
        } else if (pattern == arabicFull) {
            // цифры арабские, текст полный
            r_text = getTextArabicFull(dbeg, dend, periodType);
        } else if (pattern == arabicShort) {
            // цифры арабские, текст короткий
            r_text = getTextArabicShort(dbeg, dend, periodType);
        } else if (pattern == simpleFull) {
            // цифры словами, текст полный
            r_text = getTextFull(dbeg, dend, periodType);
        } else if (pattern == simpleShort) {
            // цифры словами, текст короткий
            r_text = getTextShort(dbeg, dend, periodType);
        } else if (pattern == hiddenParentFull) {
            // Скрытый родительский период (год)
            r_text = getHiddenParent(dbeg, dend, periodType);
        }

        return r_text;
    }

    /*
     * Генерирует полное название периода, вместо чисел - слова
     * Для: полугодие, полугодие с накоплением, квартал, квартал с накоплением,
     * декада, декада с накоплением.
     */
    protected String getTextFull(XDate dbeg, XDate dend, long periodType) {
        UtPeriod pu = new UtPeriod();
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("Первое полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("Второе полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("Первый квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("Второй квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("Третий квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("Четвертый квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.decade) {
            //Декада
            if (isDecade(dbeg, dend)) {
                r_text = UtLang.t("{0}-декада {1} г.", String.valueOf(getDecadeNo(dbeg)), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} год", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} г.", getMonthName(dbeg.decode().getMonth() - 1, false, false), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.week) {
            // Неделя
            if (isWeek(dbeg, dend)) {
                //r_text = UtLang.t("{0}-неделя {1} г.", dbeg.getWeekOfWeekyear(), String.valueOf(dbeg.getYear() != dend.getYear() ? dend.getYear() : dbeg.getYear()));
                r_text = UtLang.t("{0}-неделя {1} г.", pu.getNumWeekOfYear(dbeg), String.valueOf(dend.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            if (isDay(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} {2} г.", String.valueOf(dbeg.decode().getDay()),
                        getMonthName(dbeg.decode().getMonth() - 1, false, true).toLowerCase(), String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }

    protected String getTextShort(XDate dbeg, XDate dend, long periodType) {
        UtPeriod pu = new UtPeriod();
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("Пер. полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("Вт. полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("Пер. квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("Вт. квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("Тр. квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("Четв. квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.decade) {
            //Декада
            if (isDecade(dbeg, dend)) {
                r_text = UtLang.t("{0}-дек. {1} г.", String.valueOf(getDecadeNo(dbeg)), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} г.", getMonthName(dbeg.decode().getMonth() - 1, true, false), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.week) {
            // Неделя
            if (isWeek(dbeg, dend)) {
                //r_text = UtLang.t("{0}-неделя {1} г.", dbeg.getWeekOfWeekyear(), String.valueOf(dbeg.getYear() != dend.getYear() ? dend.getYear() : dbeg.getYear()));
                r_text = UtLang.t("{0}-нед. {1} г.", pu.getNumWeekOfYear(dbeg), String.valueOf(dend.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            if (isDay(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} {2} г.", String.valueOf(dbeg.decode().getDay()),
                        getMonthName(dbeg.decode().getMonth() - 1, true, true).toLowerCase(), String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }


    //генерирует полное название периода с римскими числами
    protected String getTextRomanFull(XDate dbeg, XDate dend, long periodType) {
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("I полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("II полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("I квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("II квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("III квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("IV квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.decade) {
            //Декада
            if (isDecade(dbeg, dend)) {
                r_text = UtLang.t("{0}-декада {1} г.", String.valueOf(getDecadeNo(dbeg)), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} год", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} г.", getMonthName(dbeg.decode().getMonth() - 1, false, false), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.week) {
            // Неделя
            if (isWeek(dbeg, dend)) {
                r_text = UtLang.t("{0}-неделя {1} г.", dbeg.decode().getDow(), String.valueOf(dend.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            if (isDay(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} {2} г.", String.valueOf(dbeg.decode().getDay()),
                        getMonthName(dbeg.decode().getMonth() - 1, false, true),
                        String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }

    //генерирует короткое название периода с римскими числами
    protected String getTextRomanShort(XDate dbeg, XDate dend, long periodType) {
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("I пол. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("II. пол. {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("I кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("II кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("III кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("IV кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.decade) {
            //Декада
            if (isDecade(dbeg, dend)) {
                r_text = UtLang.t("{0}-дек. {1} г.", String.valueOf(getDecadeNo(dbeg)), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            //Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t(" {0} {1} г.", getMonthName(dbeg.decode().getMonth() - 1, true, false), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.week) {
            // Неделя
            if (isWeek(dbeg, dend)) {
                r_text = UtLang.t("{0}-нед. {1} г.", dbeg.decode().getDow(), String.valueOf(dend.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            if (isDay(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} {2} г.", String.valueOf(dbeg.decode().getMonth()),
                        getMonthName(dbeg.decode().getMonth() - 1, true, true), String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }


    //генерирует полное название периода с арабскими числами
    protected String getTextArabicFull(XDate dbeg, XDate dend, long periodType) {
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("1-полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("2-полугодие {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("1-квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("2-квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("3-квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("4-квартал {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} год", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} год", getMonthName(dbeg.decode().getMonth() - 1, false, false), String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }

    //генерирует полное название периода с арабскими числами
    protected String getTextArabicShort(XDate dbeg, XDate dend, long periodType) {
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("1-пол. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("2-пол. {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("1-кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("2-кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("3-кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("4-кв. {0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} г.", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = UtLang.t("{0} {1} г.", getMonthName(dbeg.decode().getMonth() - 1, true, false), String.valueOf(dbeg.decode().getYear()));
            }
        }

        return r_text;
    }

    protected String getHiddenParent(XDate dbeg, XDate dend, long periodType) {
        String r_text = getTextCustom(dbeg, dend);

        if (periodType == FD_PeriodType_consts.halfyear) {
            // Полугодие
            if (isHalfYear1(dbeg, dend)) {
                r_text = UtLang.t("Первое полугодие");
            } else if (isHalfYear2(dbeg, dend)) {
                r_text = UtLang.t("Второе полугодие");
            }
        } else if (periodType == FD_PeriodType_consts.quarter) {
            // Квартал
            if (isQuarter1(dbeg, dend)) {
                r_text = UtLang.t("Первый квартал");
            } else if (isQuarter2(dbeg, dend)) {
                r_text = UtLang.t("Второй квартал");
            } else if (isQuarter3(dbeg, dend)) {
                r_text = UtLang.t("Третий квартал");
            } else if (isQuarter4(dbeg, dend)) {
                r_text = UtLang.t("Четвертый квартал");
            }
        } else if (periodType == FD_PeriodType_consts.decade) {
            //Декада
            if (isDecade(dbeg, dend)) {
                r_text = UtLang.t("{0}-декада", String.valueOf(getDecadeNo(dbeg)));
            }
        } else if (periodType == FD_PeriodType_consts.year) {
            // Год
            if (isYear(dbeg, dend)) {
                r_text = UtLang.t("{0} год", String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.month) {
            // Месяц
            if (isMonth(dbeg, dend)) {
                r_text = getMonthName(dbeg.decode().getMonth() - 1, false, false);
            }
        } else if (periodType == FD_PeriodType_consts.week) {
            // Неделя
            if (isWeek(dbeg, dend)) {
                r_text = UtLang.t("{0}-неделя", dbeg.decode().getDow());
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            if (isDay(dbeg, dend)) {
                r_text = String.valueOf(dbeg.decode().getMonth());
            }
        }

        return r_text;
    }

    /**
     * Возвращает имя месяца на русском языке
     *
     * @param isShort:    true - краткое имя, false - полное имя
     * @param MonthNumber - номер месяца (начинается с нуля, например Январь - 0, Декабрь - 11)
     * @param decline     - true - склонять название месяца в падеж, false - не склонять
     * @return
     */
    protected String getMonthName(int MonthNumber, boolean isShort, boolean decline) {

        String r_text = "";

        if (isShort) {
            switch (MonthNumber) {
                case 0 -> r_text = UtLang.t("Янв");
                case 1 -> r_text = UtLang.t("Фев");
                case 2 -> r_text = UtLang.t("Мар");
                case 3 -> r_text = UtLang.t("Апр");
                case 4 -> r_text = UtLang.t("Май");
                case 5 -> r_text = UtLang.t("Июн");
                case 6 -> r_text = UtLang.t("Июл");
                case 7 -> r_text = UtLang.t("Авг");
                case 8 -> r_text = UtLang.t("Сен");
                case 9 -> r_text = UtLang.t("Окт");
                case 10 -> r_text = UtLang.t("Ноя");
                case 11 -> r_text = UtLang.t("Дек");
            }
        } else {
            r_text = switch (MonthNumber) {
                case 0 -> decline ? UtLang.t("Января") : UtLang.t("Январь");
                case 1 -> decline ? UtLang.t("Февраля") : UtLang.t("Февраль");
                case 2 -> decline ? UtLang.t("Марта") : UtLang.t("Март");
                case 3 -> decline ? UtLang.t("Апреля") : UtLang.t("Апрель");
                case 4 -> decline ? UtLang.t("Мая") : UtLang.t("Май");
                case 5 -> decline ? UtLang.t("Июня") : UtLang.t("Июнь");
                case 6 -> decline ? UtLang.t("Июля") : UtLang.t("Июль");
                case 7 -> decline ? UtLang.t("Августа") : UtLang.t("Август");
                case 8 -> decline ? UtLang.t("Сентября") : UtLang.t("Сентябрь");
                case 9 -> decline ? UtLang.t("Октября") : UtLang.t("Октябрь");
                case 10 -> decline ? UtLang.t("Ноября") : UtLang.t("Ноябрь");
                case 11 -> decline ? UtLang.t("Декабря") : UtLang.t("Декабрь");
                default -> r_text;
            };
        }
        return r_text;
    }


    //проверка ГОДА
    protected Boolean isYear(XDate dbeg, XDate dend) {
        return dbeg.equals(dbeg.beginOfYear()) && dend.equals(dend.endOfYear());
    }

    //проверка ПЕРВАЯ ПОЛОВИНА ПОЛУГОДИЯ
    protected Boolean isHalfYear1(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().equals(dbeg) && dbeg.beginOfYear().endOfMonth().addMonths(5).equals(dend);
    }

    //проверка ВТОРАЯ ПОЛОВИНА ПОЛУГОДИЯ
    protected Boolean isHalfYear2(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(6).equals(dbeg) && dend.equals(dend.endOfYear());
    }

    //проверка ПЕРВЫЙ КВАРТАЛ
    protected Boolean isQuarter1(XDate dbeg, XDate dend) {
        return dbeg.equals(dbeg.beginOfYear()) && dbeg.addMonths(2).endOfMonth().equals(dend);
    }

    //проверка ВТОРОЙ КВАРТАЛ
    protected Boolean isQuarter2(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(3).equals(dbeg) && dbeg.beginOfYear().addMonths(5).endOfMonth().equals(dend);
    }

    //проверка ТРЕТИЙ КВАРТАЛ
    protected Boolean isQuarter3(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(6).equals(dbeg) && dbeg.beginOfYear().addMonths(8).endOfMonth().equals(dend);
    }

    //проверка ЧЕТВЕРТЫЙ КВАРТАЛ
    protected Boolean isQuarter4(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(9).equals(dbeg) && dend.equals(dend.endOfYear());
    }

    //проверка МЕСЯЦ
    protected Boolean isMonth(XDate dbeg, XDate dend) {
        return dbeg.equals(dbeg.beginOfMonth()) && dend.equals(dend.endOfMonth());
    }

    //проверка ДЕКАДА
    protected Boolean isDecade(XDate dbeg, XDate dend) {
        return dbeg.toJavaLocalDate().getDayOfMonth() == 1 && dbeg.addDays(9).equals(dend)
                || dbeg.toJavaLocalDate().getDayOfMonth() == 11 && dbeg.addDays(9).equals(dend)
                || dbeg.toJavaLocalDate().getDayOfMonth() == 21 && dend.equals(dend.endOfMonth());
    }

    //возвращает номер декады
    protected int getDecadeNo(XDate dbeg) {

        int month = dbeg.decode().getMonth() - 1;
        int tenday = (dbeg.decode().getDay() + 9) / 10;

        return month * 3 + tenday;
    }

    //проверка НЕДЕЛЯ
    protected Boolean isWeek(XDate dbeg, XDate dend) {
        return dbeg.decode().getDow() == 1 && dbeg.addDays(6).equals(dend);
    }

    //проверка ДЕНЬ
    protected Boolean isDay(XDate dbeg, XDate dend) {
        return dbeg.equals(dend);
    }

    /*
     * название периода при ошибочном вводе данных
     */
    protected String getTextCustom(XDate dbeg, XDate dend) {
        //UtDateTime.createFormatter("dd.MM.yyyy");

        return dbeg.toString(UtDateTime.createFormatter("dd.MM.yyyy")) +
                " - " + dend.toString(UtDateTime.createFormatter("dd.MM.yyyy"));
    }

}

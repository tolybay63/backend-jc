package tofi.api.dta.model.utils;

import jandcode.commons.UtDateTime;
import jandcode.commons.UtLang;
import jandcode.commons.datetime.XDate;
import tofi.api.mdl.model.consts.FD_PeriodType_consts;


public class PeriodGenerator {

    public static final long arabicFull = 3;
    public static final long arabicShort = 4;


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

        if (pattern == arabicFull) {
            // цифры арабские, текст полный
            r_text = getTextArabicFull(dbeg, dend, periodType);
        } else if (pattern == arabicShort) {
            // цифры арабские, текст короткий
            r_text = getTextArabicShort(dbeg, dend, periodType);
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
                r_text = UtLang.t("{0} {1} год", getMonthName(dbeg.decode().getMonth()-1, false, false), String.valueOf(dbeg.decode().getYear()));
            }
        } else if (periodType == FD_PeriodType_consts.day) {
            // День
            String day = String.valueOf(dbeg.decode().getDay());
            String month = "";
            switch (dbeg.decode().getMonth()) {
                case 1 -> month = UtLang.t("Январь");
                case 2 -> month = UtLang.t("Февраль");
                case 3 -> month = UtLang.t("Март");
                case 4 -> month = UtLang.t("Апрель");
                case 5 -> month = UtLang.t("Май");
                case 6 -> month = UtLang.t("Июнь");
                case 7 -> month = UtLang.t("Июль");
                case 8 -> month = UtLang.t("Август");
                case 9 -> month = UtLang.t("Сентябрь");
                case 10 -> month = UtLang.t("Октябрь");
                case 11 -> month = UtLang.t("Ноябрь");
                case 12 -> month = UtLang.t("Декабрь");
            }
            r_text = day + " " + month + " " + dbeg.decode().getYear();
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
                r_text = UtLang.t("{0} {1} г.", getMonthName(dbeg.decode().getMonth()-1, true, false), String.valueOf(dbeg.decode().getYear()));
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
     * @return name of month
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
        return dbeg.equals(dbeg.beginOfYear()) && dbeg.addMonths(3).addDays(-1).equals(dend);
    }

    //проверка ВТОРОЙ КВАРТАЛ
    protected Boolean isQuarter2(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(3).equals(dbeg) && dbeg.beginOfYear().addMonths(6).addDays(-1).equals(dend);
    }

    //проверка ТРЕТИЙ КВАРТАЛ
    protected Boolean isQuarter3(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(6).equals(dbeg) && dbeg.beginOfYear().addMonths(9).addDays(-1).equals(dend);
    }

    //проверка ЧЕТВЕРТЫЙ КВАРТАЛ
    protected Boolean isQuarter4(XDate dbeg, XDate dend) {
        return dbeg.beginOfYear().addMonths(9).equals(dbeg) && dend.equals(dend.endOfYear());
    }

    //проверка МЕСЯЦ
    protected Boolean isMonth(XDate dbeg, XDate dend) {
        return dbeg.equals(dbeg.beginOfMonth()) && dend.equals(dend.endOfMonth());
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

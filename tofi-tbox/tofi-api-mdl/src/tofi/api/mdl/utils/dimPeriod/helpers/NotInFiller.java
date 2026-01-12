package tofi.api.mdl.utils.dimPeriod.helpers;

import jandcode.commons.UtCnv;
import jandcode.commons.UtLang;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.api.mdl.model.consts.*;


import java.util.*;

public class NotInFiller {

    IPeriodItem parentBuilder;
    Store store;

    public NotInFiller(Store store, IPeriodItem parentBuilder) {
        this.parentBuilder = parentBuilder;
        this.store = store;
    }

    public Store load(String link) throws Exception {

        long periodType = parentBuilder.getPeriodType(link);
        addRecords(link);

        if (periodType == FD_PeriodType_consts.month
                || periodType == FD_PeriodType_consts.monthaccum) {
            fillNameByMonth();
        } else if (periodType == FD_PeriodType_consts.quarter
                || periodType == FD_PeriodType_consts.quarteraccum) {
            fillNameByQuarter();
        } else if (periodType == FD_PeriodType_consts.halfyear
                || periodType == FD_PeriodType_consts.halfyearaccum) {
            fillNameByHalfYear();
        } else if (periodType == FD_PeriodType_consts.decade
                || periodType == FD_PeriodType_consts.decadeaccum) {
            fillNameByDecade();
        } else if (periodType == FD_PeriodType_consts.week
                || periodType == FD_PeriodType_consts.weekaccum) {
            fillNameByWeek();
        } else if (periodType == FD_PeriodType_consts.day
                || periodType == FD_PeriodType_consts.dayaccum) {
            fillNameByDay();
        } else if (periodType == FD_PeriodType_consts.semester) {
            fillNameBySemester();
        }

        for (StoreRecord rec : store) {
            rec.setValue("checked", false);
            for (long numb : parentBuilder.getNotInList(link)) {
                if (numb == rec.getLong("id")) {
                    rec.setValue("checked", true);
                }
            }
        }
        return store;
    }

    /**
     * Переводит номера периодов в названия месяцев
     */
    protected void fillNameByMonth() {
        for (StoreRecord r : store) {
            String name = "";
            if (r.getLong("id") == 1) {
                name = UtLang.t("Январь");
            } else if (r.getLong("id") == 2) {
                name = UtLang.t("Февраль");
            } else if (r.getLong("id") == 3) {
                name = UtLang.t("Март");
            } else if (r.getLong("id") == 4) {
                name = UtLang.t("Апрель");
            } else if (r.getLong("id") == 5) {
                name = UtLang.t("Май");
            } else if (r.getLong("id") == 6) {
                name = UtLang.t("Июнь");
            } else if (r.getLong("id") == 7) {
                name = UtLang.t("Июль");
            } else if (r.getLong("id") == 8) {
                name = UtLang.t("Август");
            } else if (r.getLong("id") == 9) {
                name = UtLang.t("Сентябрь");
            } else if (r.getLong("id") == 10) {
                name = UtLang.t("Октябрь");
            } else if (r.getLong("id") == 11) {
                name = UtLang.t("Ноябрь");
            } else if (r.getLong("id") == 12) {
                name = UtLang.t("Декабрь");
            }
            r.setValue("name", name);
        }
    }

    /**
     * Переводит номера периодов в названия кварталов
     */
    protected void fillNameByQuarter() {

        for (StoreRecord r : store) {
            String name = "";
            if (r.getLong("id") == 1) {
                name = UtLang.t("Первый квартал");
            } else if (r.getLong("id") == 2) {
                name = UtLang.t("Второй квартал");
            } else if (r.getLong("id") == 3) {
                name = UtLang.t("Третий квартал");
            } else if (r.getLong("id") == 4) {
                name = UtLang.t("Четвертый квартал");
            }
            r.setValue("name", name);
        }
    }

    /**
     * Переводит номера периодов в названия полугодии
     */
    protected void fillNameByHalfYear() {

        for (StoreRecord r : store) {
            String name = "";
            if (r.getLong("id") == 1) {
                name = UtLang.t("Первое полугодие");
            } else if (r.getLong("id") == 2) {
                name = UtLang.t("Второе полугодие");
            }
            r.setValue("name", name);
        }
    }

    /**
     * Переводит номера периодов в названия декад
     */
    protected void fillNameByDecade() {

        for (StoreRecord r : store) {
            String name = UtLang.t("{0}-декада", String.valueOf(r.getLong("id")));
            r.setValue("name", name);
        }

    }

    /**
     * Переводит номера периодов в названия недель
     */
    protected void fillNameByWeek() {

        for (StoreRecord r : store) {
            String name = UtLang.t("{0}-неделя", String.valueOf(r.getLong("id")));
            r.setValue("name", name);
        }

    }

    /**
     * Переводит номера периодов в названия дней
     */
    protected void fillNameByDay() {

        for (StoreRecord r : store) {
            String name = UtLang.t("{0}-день", String.valueOf(r.getLong("id")));
            r.setValue("name", name);
        }

    }

    /**
     * Переводит номера периодов в названия семестров
     */
    protected void fillNameBySemester() {

        for (StoreRecord r : store) {
            String name = "";
            if (r.getLong("id") == 1) {
                name = UtLang.t("Первый семестр");
            } else if (r.getLong("id") == 2) {
                name = UtLang.t("Второй семестр");
            }
            r.setValue("name", name);
        }
    }

    /**
     * Заполняет переданное множество (periods) исключенными периодами (номерами периодов).
     * Рекурсивоно "передвигаясь" по деревоу узлов измерения, переводит исключенные периоды на периоды нужного
     * типа (параметр periodType).
     *
     * @param parent     ссылка на родительский уровень
     * @param periodType тип периода
     * @param periods    множество содержащий номера периодов
     */
    protected void fill(String parent, long periodType, Set<Long> periods) throws Exception {

        if (parent != null) {

            long parentPeriodType = parentBuilder.getPeriodType(parent);

            if (parentPeriodType == FD_PeriodType_consts.year
                    || parentPeriodType == FD_PeriodType_consts.ninemonth
                    || parentPeriodType == FD_PeriodType_consts.nonstandard) {
                // если родитель год, 9 месяцев или нестандартный период, тогда выходим
                return;
            }

            List<Long> list = parentBuilder.getNotInList(parent);

            //DimPeriodItem_util u = new DimPeriodItem_util()
            for (long no : list) {
                Set<Long> set = getChildPeriods(no, parentPeriodType, periodType);
                periods.addAll(set);
            }

            parent = parentBuilder.getParentLink(parent);
            fill(parent, periodType, periods);
        }
    }

    /**
     * Возвращает список (множество) дочерних периодов
     *
     * @param no           порядковый номер периода в году
     * @param parentPeriod тип периода родителя
     * @param childPeriod  тип периода дочки
     * @return дочерние периоды
     * <p>
     * Например, при no=1, parentPeriod=21 (полугодие), childPeriod=31 (квартал)
     * возвращает множество [1,2], т.е. в первом полугодии два квартала
     */
    protected Set<Long> getChildPeriods(long no, long parentPeriod, long childPeriod) {

        if (!isChildPeriod(childPeriod, parentPeriod)) {
            return new HashSet<Long>();
        }

        if (childPeriod == FD_PeriodType_consts.week
                || childPeriod == FD_PeriodType_consts.weekaccum) {
            return weeks(no, parentPeriod);
        } else if (childPeriod == FD_PeriodType_consts.day
                || childPeriod == FD_PeriodType_consts.dayaccum) {
            return days(no, parentPeriod);
        } else {
            return normalPeriods(no, parentPeriod, childPeriod);
        }

    }

    protected Set<Long> days(long no, long parent) {
        return new HashSet<Long>();
    }

    protected Set<Long> weeks(long no, long parent) {
        return new HashSet<Long>();
    }

    /**
     * Проверяет типы периодов, является ли первый тип дочерним второго
     *
     * @param child
     * @param parent
     * @return
     */
    protected Boolean isChildPeriod(long child, long parent) {

        if (parent == 0) return true;

        List<Map<String, Object>> periods = new ArrayList<Map<String, Object>>(); //[];

        if (parent == FD_PeriodType_consts.academicyear || parent == FD_PeriodType_consts.semester) {
            periods.add(Map.of("level", 1, "period", FD_PeriodType_consts.academicyear, "acc", true));
            periods.add(Map.of("level", 2, "period", FD_PeriodType_consts.semester, "acc", false));
            //periods.add([level: 1, period: FD_Const.PeriodType_year_A, acc: true])
            //periods.add([level: 2, period: FD_Const.PeriodType_semester, acc: false])
        } else {
            periods.add(Map.of("level", 1, "period", FD_PeriodType_consts.year, "acc", true));
            periods.add(Map.of("level", 1, "period", FD_PeriodType_consts.ninemonth, "acc", true));
            //periods.add([level: 1, period: FD_Const.PeriodType_year, acc: true])
            //periods.add([level: 1, period: FD_Const.PeriodType_n_month, acc: true])
            periods.add(Map.of("level", 2, "period", FD_PeriodType_consts.halfyear, "acc", false));
            periods.add(Map.of("level", 2, "period", FD_PeriodType_consts.halfyearaccum, "acc", true));
            //periods.add([level: 2, period: FD_Const.PeriodType_halfYear, acc: false])
            //periods.add([level: 2, period: FD_Const.PeriodType_halfYear_A, acc: true])
            periods.add(Map.of("level", 3, "period", FD_PeriodType_consts.quarter, "acc", false));
            periods.add(Map.of("level", 3, "period", FD_PeriodType_consts.quarteraccum, "acc", true));
            //periods.add([level: 3, period: FD_Const.PeriodType_quarter, acc: false])
            //periods.add([level: 3, period: FD_Const.PeriodType_quarter_A, acc: true])
            periods.add(Map.of("level", 4, "period", FD_PeriodType_consts.month, "acc", false));
            periods.add(Map.of("level", 4, "period", FD_PeriodType_consts.monthaccum, "acc", true));
            //periods.add([level: 4, period: FD_Const.PeriodType_month, acc: false])
            //periods.add([level: 4, period: FD_Const.PeriodType_month_A, acc: true])
            periods.add(Map.of("level", 5, "period", FD_PeriodType_consts.decade, "acc", false));
            periods.add(Map.of("level", 5, "period", FD_PeriodType_consts.decadeaccum, "acc", true));
            //periods.add([level: 5, period: FD_Const.PeriodType_tenDay, acc: false])
            //periods.add([level: 5, period: FD_Const.PeriodType_tenDay_A, acc: true])
            periods.add(Map.of("level", 5, "period", FD_PeriodType_consts.week, "acc", false));
            periods.add(Map.of("level", 5, "period", FD_PeriodType_consts.weekaccum, "acc", true));
            //periods.add([level: 5, period: FD_Const.PeriodType_week, acc: false])
            //periods.add([level: 5, period: FD_Const.PeriodType_week_A, acc: true])
            periods.add(Map.of("level", 6, "period", FD_PeriodType_consts.day, "acc", false));
            periods.add(Map.of("level", 6, "period", FD_PeriodType_consts.dayaccum, "acc", true));
            //periods.add([level: 6, period: FD_Const.PeriodType_day, acc: false])
            //periods.add([level: 6, period: FD_Const.PeriodType_day_A, acc: true])
        }

        int childLevel = 0, parentLevel = 0;
        boolean isChildAcc = false, isParentAcc = false;

        for (Map<String, Object> m : periods) {
            if (m.get("period").equals(child)) {
                childLevel = UtCnv.toInt(m.get("level"));
                isChildAcc = UtCnv.toBoolean(m.get("acc"));
            }
            if (m.get("period").equals(parent)) {
                parentLevel = UtCnv.toInt(m.get("level"));
                isParentAcc = UtCnv.toBoolean(m.get("acc"));
            }
        }

        //return parentLevel < childLevel && (isParentAcc || (!isParentAcc && !isChildAcc));
        return parentLevel < childLevel && (isParentAcc || !isChildAcc);
    }

    /**
     * Возвращает исключенные периоды родительского уровня переведенного в тип периода дочерноего уровня,
     * если дочерний период не является типом периода "неделя" и "день".
     *
     * @param no
     * @param parentPeriod
     * @param childPeriod
     * @return
     */
    protected Set<Long> normalPeriods(long no, long parentPeriod, long childPeriod) {

        Map<String, Object> year = Map.of("halfYear", 2, "quarter", 4, "month", 12, "decade", 36, "day", 31, "max", 1);
        Map<String, Object> nineMonths = Map.of("halfYear", 1, "quarter", 3, "month", 9, "decade", 27, "day", 31, "max", 1);
        Map<String, Object> halfYear = Map.of("quarter", 2, "month", 6, "decade", 18, "day", 31, "max", 2);
        Map<String, Object> quarter = Map.of("month", 3, "decade", 9, "day", 31, "max", 4);
        Map<String, Object> month = Map.of("decade", 3, "day", 31, "max", 12);
        Map<String, Object> decade = Map.of("max", 36);
        Map<String, Object> academicYear = Map.of("semester", 2, "max", 1);

        Map<String, Object> parent = new HashMap<>();
        if (parentPeriod == FD_PeriodType_consts.year) {
            parent = year;
        } else if (parentPeriod == FD_PeriodType_consts.ninemonth) {
            parent = nineMonths;
        } else if (parentPeriod == FD_PeriodType_consts.halfyear
                || parentPeriod == FD_PeriodType_consts.halfyearaccum) {
            parent = halfYear;
        } else if (parentPeriod == FD_PeriodType_consts.quarter
                || parentPeriod == FD_PeriodType_consts.quarteraccum) {
            parent = quarter;
        } else if (parentPeriod == FD_PeriodType_consts.month
                || parentPeriod == FD_PeriodType_consts.monthaccum) {
            parent = month;
        } else if (parentPeriod == FD_PeriodType_consts.decade) {
            parent = decade;
        } else if (parentPeriod == FD_PeriodType_consts.academicyear) {
            parent = academicYear;
        }

        String attr = "";
        if (childPeriod == FD_PeriodType_consts.halfyear
                || childPeriod == FD_PeriodType_consts.halfyearaccum) {
            attr = "halfYear";
        } else if (childPeriod == FD_PeriodType_consts.quarter
                || childPeriod == FD_PeriodType_consts.quarteraccum) {
            attr = "quarter";
        } else if (childPeriod == FD_PeriodType_consts.month
                || childPeriod == FD_PeriodType_consts.monthaccum) {
            attr = "month";
        } else if (childPeriod == FD_PeriodType_consts.decade
                || childPeriod == FD_PeriodType_consts.decadeaccum) {
            attr = "decade";
        } else if (childPeriod == FD_PeriodType_consts.semester) {
            attr = "semester";
        }

        long count = UtCnv.toLong(parent.get(attr));
        long from = (no - 1) * count + 1;
        long to = (no * count);

        if (no > UtCnv.toInt(parent.get("max")) || no < 1) {
            from = 1;
            to = count;
        }

        Set<Long> periods = new HashSet<Long>();
        for (long i = from; i <= to; i++) {
            periods.add(i);
        }
        //
        return periods;
    }

    /**
     * Добавляет в сторе записями исключенных периодов верхними уровнями.
     * Например, предположим, имется на верхнем уровне кварталы, а на нижнем - месяцы. Если был исключен первый квартал,
     * в сторе добавляются 3 записи соответсвующие к месяцам Январь, Февраль и Март.
     *
     * @param link ссылка (id) на узел периода
     */
    protected void addRecords(String link) throws Exception {

        long periodType = parentBuilder.getPeriodType(link);
        Set<Long> notIn = new TreeSet<Long>();

        fill(parentBuilder.getParentLink(link), periodType, notIn);

        Set<Long> set = new TreeSet<Long>();
        long max = 0;

        if (periodType == FD_PeriodType_consts.halfyear
                || periodType == FD_PeriodType_consts.halfyearaccum) {
            max = 2;
        } else if (periodType == FD_PeriodType_consts.quarter
                || periodType == FD_PeriodType_consts.quarteraccum) {
            max = 4;
        } else if (periodType == FD_PeriodType_consts.month
                || periodType == FD_PeriodType_consts.monthaccum) {
            max = 12;
        } else if (periodType == FD_PeriodType_consts.decade
                || periodType == FD_PeriodType_consts.decadeaccum) {
            max = 36;
        } else if (periodType == FD_PeriodType_consts.week
                || periodType == FD_PeriodType_consts.weekaccum) {
            max = 53;
        } else if (periodType == FD_PeriodType_consts.day
                || periodType == FD_PeriodType_consts.dayaccum) {
            max = 31;
        } else if (periodType == FD_PeriodType_consts.semester) {
            max = 2;
        }

        for (long i = 1; i <= max; i++) {
            set.add(i);
        }

        set.removeAll(notIn);

        for (Long i : set) {
            store.add(Map.of("id", i, "name", String.valueOf(i)));
        }
    }

}

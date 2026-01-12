package tofi.api.mdl.utils.dimPeriod.helpers;

import jandcode.commons.UtCnv;
import jandcode.commons.datetime.XDate;
import jandcode.commons.datetime.XDateTimeFormatter;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.api.mdl.utils.AppConst;
import tofi.api.mdl.utils.UtPeriod;
import tofi.api.mdl.utils.dimPeriod.PeriodGenerator;
import tofi.api.mdl.utils.dimPeriod.TofiPeriod;
import tofi.api.mdl.model.consts.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PeriodItemTree {

    private static final String splitter = "s";
    private String domain;
    private Store store;
    private static final String dateFormat = "yyyy-MM-dd";
    private Mdb mdb;
    private IPeriodElement periodElement;
    private long periodTml;
    private XDate currDate = XDate.today();


    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setPeriodElement(IPeriodElement periodElement) {
        this.periodElement = periodElement;
    }

    public void setPeriodTml(long periodTml) {
        this.periodTml = periodTml;
    }

    public void setStore(Store store) {
        this.store = store;
    }

    public void setUt(Mdb mdb) {
        this.mdb = mdb;
    }

    public void setCurrDate(XDate currDate) {
        this.currDate = currDate;
    }

    public void setCurrDate(String currDate) {
        try {
            this.currDate = XDate.create(currDate);
        } catch (Exception e) {
            this.currDate = XDate.today();
        }
    }

    public static String getSplitter() {
        return splitter;
    }

    public static String extractPeriodItemId(String id) {
        if (id != null) {
            return id.split(splitter)[0];
        } else
            return null;
    }

    public Store loadPeriodTree(Map<String, Object> params) throws Exception {
        checkInit();

        String parent = null;
        if (params.containsKey("node"))
            parent = UtCnv.toString(params.get("node"));

        if (params.containsKey("currDate")) {
            setCurrDate(UtCnv.toString(params.get("currDate")));
        }

        store = mdb.createStore(domain);
        makeTree(parent);
        store = afterBuilded(store);

        return store;
    }

    protected void checkInit() throws Exception {
        if (domain.isEmpty())
            throw new XError("Свойство domain не установлен");

        if (mdb == null)
            throw new XError("Свойство mdb не установлен");

        if (periodElement == null)
            throw new XError("Свойство periodElement не установлен");

        if (periodTml == 0)
            throw new XError("Свойство periodTml не установлен");
    }

    /*Дерево периодов*/
    protected void makeTree(String parentId) throws Exception {
        //dbeg, dend элемента вытаскиваем из id родительского элемента
        XDate p_dbeg = PeriodItemTree.extractDbeg(parentId);
        XDate p_dend = PeriodItemTree.extractDend(parentId);

        PeriodGenerator g = new PeriodGenerator();

        for (StoreRecord r : periodElement.getItemChildRec(parentId)) {
            XDate dbeg = getDbegRecur(r, currDate);
            XDate dend = getDendRecur(r, currDate);
            long periodType = r.getLong("periodType");
            long tml = r.getLong("periodNameTml");
            long lastPeriods = r.getLong("countPeriod");

            if (dbeg.toJavaLocalDate().isAfter(dend.toJavaLocalDate())) {
                // проверка - если дата начало старше даты конца, тогда не создаем периодов
                continue;
            }

            if (tml == 0) {
                tml = periodTml;
            }

            if (r.isValueNull("parent")) {
                String id = r.getString("id");
                //String p = null;
                long includeTag = r.getLong("periodIncludeTag");
                if (includeTag == 0) {
                    includeTag = FD_PeriodIncludeTag_consts.dbegAndDend;
                }

                //создаем периоды
                List<TofiPeriod> pp = g.genPeriods(dbeg, dend, periodType, includeTag, tml);

                //если задано количество периодов (поле countPeriod), удаляем лишние
                delUnnessPeriods(pp, lastPeriods);

                //переводим периоды в записи таблицы
                List<StoreRecord> periods = periodsToRecords(pp, r, null);

                //если есть исключаемые элементы - удаляем ненужные
                filterPeriods(periods, r);

                //записываем в резудьтатирующий сторе
                store.getRecords().addAll(periods);
            } else {
                String id = r.getString("id");
                long includeTag = r.getLong("periodIncludeTag");

//                if (periodType!=FD_PeriodType_consts.week) {
                if (dbeg.toJavaLocalDate().isAfter(p_dend.toJavaLocalDate())
                        || dend.toJavaLocalDate().isBefore(p_dbeg.toJavaLocalDate())) {
                    // если период не относится к данному диапазону не создаем ничего
                    continue;
                }
/*
                } else {
                    if (includeTag == FD_PeriodIncludeTag_consts.dbeg && dbeg.toJavaLocalDate().isBefore(p_dbeg.toJavaLocalDate())) {
                        continue;
                    }
                    if (includeTag == FD_PeriodIncludeTag_consts.dend && dend.toJavaLocalDate().isAfter(p_dend.toJavaLocalDate())) {
                        continue;
                    }
                    if (includeTag == FD_PeriodIncludeTag_consts.dbegAndDend &&
                            (dbeg.toJavaLocalDate().isBefore(p_dbeg.toJavaLocalDate()) || dend.toJavaLocalDate().isAfter(p_dend.toJavaLocalDate()))) {
                        continue;
                    }
                }
*/

                /* сначал создаем все периоды элемента измерения
                для определения всего количества
                 */
                List<TofiPeriod> pp = g.genPeriods(dbeg, dend, periodType, includeTag, tml);

                if (dbeg.toJavaLocalDate().isBefore(p_dbeg.toJavaLocalDate())) {
                    dbeg = p_dbeg;
                }
                if (dend.toJavaLocalDate().isAfter(p_dend.toJavaLocalDate())) {
                    dend = p_dend;
                }

                /*
                создаем второй список периодов уже с диапазоном который входит
                в родительский период
                 */
                List<TofiPeriod> pp2 = g.genPeriods(dbeg, dend, periodType, includeTag, tml);

                delUnnessPeriods(pp, lastPeriods);

                /*
                не совпадающие периоды удаляем из списка
                 */
                List<TofiPeriod> delList = new ArrayList<TofiPeriod>();
                for (TofiPeriod p : pp2) {

                    boolean exist = false;

                    for (TofiPeriod p2 : pp) {
                        if (p.getDbeg().toJavaLocalDate().isEqual(p2.getDbeg().toJavaLocalDate())
                                && p.getDend().toJavaLocalDate().isEqual(p2.getDend().toJavaLocalDate())) {
                            exist = true;
                        }
                    }

                    if (!exist) {
                        delList.add(p);
                    }
                }
                pp2.removeAll(delList);

                List<StoreRecord> periods = periodsToRecords(pp2, r, parentId);
                filterPeriods(periods, r);
                store.getRecords().addAll(periods);
            }

        }
    }

    /**
     * Фильтрует список периодов по списку исключенных периодов
     */
    protected void filterPeriods(List<StoreRecord> periods, StoreRecord rec) throws Exception {

        List<StoreRecord> delList = new ArrayList<StoreRecord>();
        List<Long> list = periodElement.getNotInList(rec.getString("id"));
        for (Long no : list) {
            for (StoreRecord period : periods) {
                if (getNumb(period) == no) {
                    delList.add(period);
                }
            }
        }
        periods.removeAll(delList);
    }

    /**
     * Возвращает номер заданного периода в году
     */
    protected long getNumb(StoreRecord r) {

        long no = 1;
        long periodType = r.getLong("PeriodType");
        UtPeriod pu = new UtPeriod();

        if (periodType == FD_PeriodType_consts.halfyear
                || periodType == FD_PeriodType_consts.halfyearaccum) {
            no = pu.getHalfYearOfYear(r.getDate("dend"));
        } else if (periodType == FD_PeriodType_consts.quarter
                || periodType == FD_PeriodType_consts.quarteraccum) {
            no = pu.getQuarterOfYear(r.getDate("dend"));
        } else if (periodType == FD_PeriodType_consts.month
                || periodType == FD_PeriodType_consts.monthaccum) {
            no = r.getDate("dend").decode().getMonth();
        } else if (periodType == FD_PeriodType_consts.week
                || periodType == FD_PeriodType_consts.weekaccum) {
            no = pu.getNumWeekOfYear(r.getDate("dend"));
        } else if (periodType == FD_PeriodType_consts.decade
                || periodType == FD_PeriodType_consts.decadeaccum) {
            no = pu.getTenDayOfYear(r.getDate("dend"));
        } else if (periodType == FD_PeriodType_consts.day
                || periodType == FD_PeriodType_consts.dayaccum) {
            no = r.getDate("dend").toJavaLocalDate().getDayOfMonth();
        } else if (periodType == FD_PeriodType_consts.semester) {
            no = pu.getSemesterOfAcademicYear(r.getDate("dend"));
        }
        return no;
    }


    /**
     * Переводит список периодов (TofiPeriod) в записи (DataRecord)
     */
    protected List<StoreRecord> periodsToRecords(List<TofiPeriod> periods, StoreRecord dimPeriodItemRec, String parent) {

        List<StoreRecord> list = new ArrayList<StoreRecord>();
        String id = dimPeriodItemRec.getString("id");

        for (TofiPeriod period : periods) {

            StoreRecord n = mdb.createStoreRecord(domain);

            n.setValue("parent", parent);
            n.setValue("name", period.getName());
            n.setValue("dbeg", period.getDbeg());
            n.setValue("dend", period.getDend());
            n.setValue("id", id + splitter + n.getDate("dbeg").toString()
                    + n.getDate("dend").toString());
            n.setValue("dimPeriodItem", Long.valueOf(id));
            n.setValue("periodType", period.getPeriodType());
            n.setValue("dimperiod", dimPeriodItemRec.getLong("dimperiod"));
            n.setValue("ord", dimPeriodItemRec.getValue("ord"));
            list.add(n);
        }

        return list;
    }


    /**
     * если начало периода определяется по заданному количеству периода,
     * тогда убираем лишние периоды
     */
    protected void delUnnessPeriods(List<TofiPeriod> periods, long lastPeriods) {

        if (lastPeriods > 0) {

            int delTo = (int) (periods.size() - lastPeriods);
            if (delTo > 0) {
                periods.subList(0, delTo).clear();
            }
        }

    }


    /**
     * Вытаскивает из id дата начало периода
     *
     * @param id
     * @return
     */
    public static XDate extractDbeg(String id) {

        if (id == null || id.isEmpty())
            return AppConst.DBEG_EMPTY;

        String[] arr = id.split(splitter);

        if (arr.length < 2)
            return AppConst.DBEG_EMPTY;

        String str = arr[1];

        if (str.length() < 20)
            return AppConst.DBEG_EMPTY;

        str = str.substring(0, 10);

        XDate dbeg;
        try {
            XDateTimeFormatter formatter = XDateTimeFormatter.ISO_DATE;
            dbeg = XDate.create(str);
        } catch (IllegalArgumentException e) {
            dbeg = AppConst.DBEG_EMPTY;
        }
        return dbeg;
    }

    /**
     * Вытаскивает из id дата конца периода
     *
     * @param id
     * @return
     */
    public static XDate extractDend(String id) {

        if (id == null || id.isEmpty())
            return AppConst.DEND_EMPTY;

        String[] arr = id.split(splitter);

        if (arr.length < 2)
            return AppConst.DEND_EMPTY;

        String str = arr[1];

        if (str.length() < 20)
            return AppConst.DEND_EMPTY;

        str = str.substring(10, 20);

        XDate dend;
        try {
            //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            dend = XDate.create(str);
        } catch (Exception e) {
            dend = AppConst.DEND_EMPTY;
        }
        return dend;
    }

    /**
     * Возвращает начало периода элемента измерения рекурсивно,
     * т.е. если у элемента не задано начало периода - возвращает начало периода
     * первого родителя, у которого задано начало периода
     */
    protected XDate getDbegRecur(StoreRecord r, XDate currDate) throws Exception {
        //сначала выясняем конец периода, т.к. countPeriod вычисляется относительно конца периода
        r.setValue("dend", getDendRecur(r, currDate));

        if (AppConst.isDateEmpty(r.getDate("dbeg"))
                && r.isValueNull("countPeriod")
                && !r.isValueNull("parent")) {
            return getDbegRecur(periodElement.getItemRec(r.getString("parent")), currDate);
        }

        DimPeriodItemUtil du = new DimPeriodItemUtil();
        //mdb.outTable(r);
        return du.getDbeg(r, r.getLong("periodType"), currDate);
    }

    /**
     * Возвращает конец периода элемента измерения рекурсивно,
     * т.е. если у элемента не задано конец периода - возвращает конец периода
     * первого родителя, у которого задано конец периода
     */
    protected XDate getDendRecur(StoreRecord r, XDate currDate) throws Exception {

        if (AppConst.isDateEmpty(r.getDate("dend"))
                && r.isValueNull("lagCurrentDate")
                && !r.isValueNull("parent")) {
            return getDendRecur(periodElement.getItemRec(r.getString("parent")), currDate);
        }

        DimPeriodItemUtil du = new DimPeriodItemUtil();
        return du.getDend(r, r.getLong("periodType"), currDate);
    }

    /**
     * вызывается после загрузки дерева
     *
     * @param st
     * @return
     */
    protected Store afterBuilded(Store st) throws Exception {

        for (StoreRecord r : st) {
            String id = r.getString("id").split(splitter)[0];
            List<StoreRecord> childRecords = periodElement.getItemChildRec(id);

            if (!childRecords.isEmpty())
                r.setValue("leaf", false);
            else
                r.setValue("leaf", true);
        }
        return st;
    }

}

package tofi.mdl.model.dao.dimperiod;

import jandcode.commons.UtCnv;
import jandcode.commons.UtLang;
import jandcode.commons.datetime.XDate;
import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.dict.DictService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_PeriodIncludeTag_consts;
import tofi.mdl.consts.FD_PeriodNameTml_consts;
import tofi.mdl.consts.FD_PeriodType_consts;
import tofi.mdl.model.dao.dimperiod.helpers.IPeriodItem;
import tofi.mdl.model.dao.dimperiod.helpers.NotInFiller;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.period.UtPeriod;

import java.util.*;

public class DimPeriodMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public DimPeriodMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }

    /**
     * Загрузка DimPeriod с пагинацией
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Map<String, Object> loadDimPeriodPaginate(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo
        long al = au.getAttrs().getLong("accesslevel");

        String sql = "select * from DimPeriod where accessLevel <= " + al;
        SqlText sqlText = mdb.createSqlText(sql);
        Map<String, Object> par = new HashMap<>();
        int offset = (UtCnv.toInt(params.get("page")) - 1) * UtCnv.toInt(params.get("limit"));
        par.put("offset", offset);
        par.put("limit", UtCnv.toInt(params.get("limit")));
        sqlText.setSql(sql);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("orderBy")).trim().isEmpty())
            sqlText = sqlText.replaceOrderBy(UtCnv.toString(params.get("orderBy")));

        String filter = UtCnv.toString(params.get("filter")).trim();
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("(cod like '%" + filter + "%' or name like '%" + filter + "%' or " +
                    "fullName like '%" + filter + "%')");
        Store st = mdb.createStore("DimPeriod");
        mdb.loadQuery(st, sqlText, par);
        mdb.resolveDicts(st);
        //count
        sql = "select count(*) as cnt from DimPeriod where accessLevel <= " + al;
        sqlText.setSql(sql);
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("name like '%" + filter + "%' or fullName like '%" + filter + "%' or cod like '%" + filter + "%'");
        int total = mdb.loadQuery(sqlText).get(0).getInt("cnt");
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", UtCnv.toInt(params.get("limit")));
        return Map.of("store", st, "meta", meta);
    }

    /**
     * Новая запись
     *
     * @param params
     * @return
     * @throws Exception
     */

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        DictService dictSvc = mdb.getModel().bean(DictService.class);
        Store st = mdb.createStore("DimPeriod");
        StoreRecord rec = st.add();
        rec.set("accessLevel", FD_AccessLevel_consts.common);
        rec.set("periodNameTml", FD_PeriodNameTml_consts.full);
        dictSvc.resolveDicts(st);
        //mdb.outTable(rec);
        return rec;
    }

    /**
     * Добавить запись
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        //
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("DimPeriod");
        mdb.loadQuery(st, "select * from DimPeriod where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    /**
     * Обновить запись
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntity(rec);
        //
        // Загрузка записи
        Store st = mdb.createStore("DimPeriod");
        mdb.loadQuery(st, "select * from DimPeriod where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    /**
     * Удалить запись
     *
     * @param map
     * @throws Exception
     */
    public void delete(Map<String, Object> map) throws Exception {
        deleteEntity(UtCnv.toMap(map.get("rec")));
    }

    //============================ DimPeriodItem ================================//
    public Store loadDPI(long dimperiod) throws Exception {
        Store st = mdb.createStore("DimPeriodItem");
        return mdb.loadQuery(st, """
                   select * from DimPeriodItem
                   where dimperiod=:dp
                """, Map.of("dp", dimperiod));
    }

    public Store newRecDPI(boolean isChild, Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("DimPeriodItem");
        StoreRecord r = st.add();

        if (!isChild) {
            r.set("periodType", FD_PeriodType_consts.year);
            r.set("countPeriod", 1);
            r.set("lagCurrentDate", 0);
            long tml = mdb.loadQuery("select periodNameTml from DimPeriod where id=:id",
                    Map.of("id", rec.get("dimperiod"))).get(0).getLong("periodNameTml");
            r.set("periodNameTml", tml);
        } else {
            r.set("periodNameTml", UtCnv.toLong(rec.get("periodNameTml")));
            long periodType = UtCnv.toLong(rec.get("periodType"));
            if (periodType == FD_PeriodType_consts.year)
                r.set("periodType", FD_PeriodType_consts.halfyear);
            else if (periodType == FD_PeriodType_consts.halfyear)
                r.set("periodType", FD_PeriodType_consts.quarter);
            else if (periodType == FD_PeriodType_consts.quarter)
                r.set("periodType", FD_PeriodType_consts.month);
            else if (periodType == FD_PeriodType_consts.month)
                r.set("periodType", FD_PeriodType_consts.decade);
            else if (periodType == FD_PeriodType_consts.decade)
                r.set("periodType", FD_PeriodType_consts.week);
            else if (periodType == FD_PeriodType_consts.week)
                r.set("periodType", FD_PeriodType_consts.day);
            else if (periodType == FD_PeriodType_consts.ninemonth)
                r.set("periodType", FD_PeriodType_consts.month);
            //
            r.set("periodIncludeTag", FD_PeriodIncludeTag_consts.dbegAndDend);
            r.set("parent", UtCnv.toLong(rec.get("id")));
        }
//mdb.outTable(st);
        return st;
    }

    public String getPeriodBeg(String dbeg, long periodType) throws Exception {
        UtPeriod ut = new UtPeriod();
        XDate dt = ut.calcDbeg(UtCnv.toDate(dbeg), periodType, 0);
        return UtCnv.toString(dt);
    }

    public String getPeriodEnd(String dend, long periodType) throws Exception {
        UtPeriod ut = new UtPeriod();
        XDate dt = ut.calcDend(UtCnv.toDate(dend), periodType, 0);
        return UtCnv.toString(dt);
    }

    public Store insertDPI(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        //
        long id = mdb.getNextId("DimPeriodItem");
        rec.put("ord", id);
        mdb.insertRec("DimPeriodItem", rec, false);
        //
        Store st = mdb.createStore("DimPeriodItem");
        mdb.loadQuery(st, "select * from DimPeriodItem where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public Store updateDPI(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        Store st = mdb.createStore("DimPeriodItem");
        StoreRecord r = st.add(rec);
        mdb.updateRec("DimPeriodItem", r);
        //
        // Загрузка записи
        st.cloneStore();
        mdb.loadQuery(st, "select * from DimPeriodItem where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public void deleteDPI(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("DimPeriodItem");
        StoreRecord r = st.add(rec);
        mdb.deleteRec("DimPeriodItem", r.getLong("id"));
    }

    //
    public Store loadNotIn(long dimPeriodItem) throws Exception {
        Store st = mdb.createStore("DimPeriodItemNotIn.full");
        mdb.loadQuery(st, """
                   select * from DimPeriodItemNotIn where dimPeriodItem=:dpi
                """, Map.of("dpi", dimPeriodItem));
        //
        for (StoreRecord r : st) {
            r.setValue("name", String.valueOf(r.getLong("numb")));
            StoreRecord rec = mdb.loadQueryRecord("select * from DimPeriodItem where id=:id", Map.of("id", dimPeriodItem));
            long periodType = rec.getLong("PeriodType");
            if (periodType == FD_PeriodType_consts.month) {
                fillNameByMonth(st);
            } else if (periodType == FD_PeriodType_consts.quarter) {
                fillNameByQuarter(st);
            } else if (periodType == FD_PeriodType_consts.halfyear) {
                fillNameByHalfYear(st);
            } else if (periodType == FD_PeriodType_consts.year) {
                fillNameByYear(st);
            } else if (periodType == FD_PeriodType_consts.week) {
                fillNameByWeek(st);
            } else if (periodType == FD_PeriodType_consts.decade) {
                fillNameTenDay(st);
            } else if (periodType == FD_PeriodType_consts.day) {
                fillNameDay(st);
            }
        }
        //
        return st;
    }

    protected void fillNameByMonth(Store st) {
        for (StoreRecord r : st) {
            if (r.getLong("numb") == 1) {
                r.setValue("name", UtLang.t("Январь"));
            } else if (r.getLong("numb") == 2) {
                r.setValue("name", UtLang.t("Февраль"));
            } else if (r.getLong("numb") == 3) {
                r.setValue("name", UtLang.t("Март"));
            } else if (r.getLong("numb") == 4) {
                r.setValue("name", UtLang.t("Апрель"));
            } else if (r.getLong("numb") == 5) {
                r.setValue("name", UtLang.t("Май"));
            } else if (r.getLong("numb") == 6) {
                r.setValue("name", UtLang.t("Июнь"));
            } else if (r.getLong("numb") == 7) {
                r.setValue("name", UtLang.t("Июль"));
            } else if (r.getLong("numb") == 8) {
                r.setValue("name", UtLang.t("Август"));
            } else if (r.getLong("numb") == 9) {
                r.setValue("name", UtLang.t("Сентябрь"));
            } else if (r.getLong("numb") == 10) {
                r.setValue("name", UtLang.t("Октябрь"));
            } else if (r.getLong("numb") == 11) {
                r.setValue("name", UtLang.t("Ноябрь"));
            } else if (r.getLong("numb") == 12) {
                r.setValue("name", UtLang.t("Декабрь"));
            }
        }
    }

    protected void fillNameByQuarter(Store st) {
        for (StoreRecord r : st) {
            if (r.getLong("numb") == 1) {
                r.setValue("name", UtLang.t("Первый квартал"));
            } else if (r.getLong("numb") == 2) {
                r.setValue("name", UtLang.t("Второй квартал"));
            } else if (r.getLong("numb") == 3) {
                r.setValue("name", UtLang.t("Третий квартал"));
            } else if (r.getLong("numb") == 4) {
                r.setValue("name", UtLang.t("Четвертый квартал"));
            }
        }
    }

    protected void fillNameByHalfYear(Store st) {
        for (StoreRecord r : st) {
            if (r.getLong("numb") == 1) {
                r.setValue("name", UtLang.t("Первое полугодие"));
            } else if (r.getLong("numb") == 2) {
                r.setValue("name", UtLang.t("Второе полугодие"));
            }
        }
    }

    protected void fillNameByYear(Store st) {
        for (StoreRecord r : st) {
            r.setValue("name", UtLang.t("{0} год", String.valueOf(r.getLong("numb"))));
        }
    }

    protected void fillNameByWeek(Store st) {
        for (StoreRecord r : st) {
            r.setValue("name", UtLang.t("{0}-неделя", String.valueOf(r.getLong("numb"))));
        }
    }

    protected void fillNameTenDay(Store st) {
        for (StoreRecord r : st) {
            r.setValue("name", UtLang.t("{0}-декада", String.valueOf(r.getLong("numb"))));
        }
    }

    protected void fillNameDay(Store st) {
        for (StoreRecord r : st) {
            r.setValue("name", UtLang.t("{0}-день", String.valueOf(r.getLong("numb"))));
        }
    }

    ////////////////////////////////////////////////
    public Store loadNotInForUpdate(long dimPeriodItem) throws Exception {
        Store res = mdb.createStore("DimPeriodItemNotIn.edit");

        NotInFiller filler = new NotInFiller(res, new IPeriodItem() {
            public long getPeriodType(String link) throws Exception {
                long id = Long.parseLong(link);
                StoreRecord rec = mdb.loadQuery("""
                            select * from DimPeriodItem where id=:id
                        """, Map.of("id", id)).get(0);
                return rec.getLong("periodType");
            }

            public List<Long> getNotInList(String link) throws Exception {
                long id = Long.parseLong(link);
                Store st = mdb.loadQuery("""
                                    select t.* from
                                    DimPeriodItemNotIn t
                                    where t.dimperiodItem=:dimperiodItem
                        """, Map.of("dimperiodItem", id));

                List<Long> list = new ArrayList<Long>();
                for (StoreRecord rec : st) {
                    list.add(rec.getLong("numb"));
                }
                return list;
            }

            public String getParentLink(String link) throws Exception {
                long id = Long.parseLong(link);
                StoreRecord record = mdb.loadQueryRecord("select * from DimPeriodItem where id=:id", Map.of("id", id));
                long parent = record.getLong("parent");

                if (parent == 0)
                    return null;
                else
                    return String.valueOf(parent);
            }
        });
        return filler.load(String.valueOf(dimPeriodItem));
    }

    //
    public void updateNotIn(long dimPeriodItem, List<Map<String, Object>> data) throws Exception {

        boolean edited = false;

        //Old Ids
        Store oldSt = mdb.loadQuery("""
                    select * from DimPeriodItemNotIn where dimperiodItem=:dpi
                """, Map.of("dpi", dimPeriodItem));

        Set<Object> oldIds = oldSt.getUniqueValues("id");

        Set<Object> newIds = new HashSet<>();
        for (Map<String, Object> map : data) {
            newIds.add(UtCnv.toLong(map.get("id")));
        }

        //Deleting...
        for (Object oldId : oldIds) {
            if (!newIds.contains(oldId)) {
                edited = true;
                mdb.deleteRec("DimPeriodItemNotIn", UtCnv.toLong(oldId));
            }
        }

        // Saving...
        for (Object newId : newIds) {
            if (!oldIds.contains(newId)) {
                edited = true;
                mdb.insertRec("DimPeriodItemNotIn",
                        Map.of("dimPeriodItem", dimPeriodItem, "numb", UtCnv.toInt(newId)), true);
            }
        }

        if (edited)
            clearChildrenNotIn(dimPeriodItem);

    }

    protected void clearChildrenNotIn(long dpi) throws Exception {
        Store st = mdb.loadQuery("select id from DimPeriodItem where parent=:id", Map.of("dpi", dpi));
        for (StoreRecord r : st) {
            mdb.execQuery("delete from DimPeriodItemNotIn where dimperioditem=:id", Map.of("id", r.getLong("id")));
            clearChildrenNotIn(r.getLong("id"));
        }
    }


}

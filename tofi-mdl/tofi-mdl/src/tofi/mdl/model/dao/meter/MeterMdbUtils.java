package tofi.mdl.model.dao.meter;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.dict.DictService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.*;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.HashMap;
import java.util.Map;


public class MeterMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;


    public MeterMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }


    /**
     * Загрузка Meter с пагинацией
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Map<String, Object> loadMeterPaginate(Map<String, Object> params) throws Exception {
        String sql0 = "select f.* from Meter f where 0=0 order by f.id";
        SqlText sqlText = mdb.createSqlText(sql0);
        String filter = UtCnv.toString(params.get("filter")).trim();

        //count
        String sql = "select count(*) as cnt from Meter f where 0=0";
        sqlText.setSql(sql);
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("name like '%" + filter + "%' or fullName like '%" + filter + "%' or cod like '%" + filter + "%'");
        int total = mdb.loadQuery(sqlText).get(0).getInt("cnt");
        int lm = UtCnv.toInt(params.get("rowsPerPage")) == 0 ? total : UtCnv.toInt(params.get("rowsPerPage"));
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", lm);

        Map<String, Object> par = new HashMap<>();
        int offset = (UtCnv.toInt(params.get("page")) - 1) * lm;
        par.put("offset", offset);
        par.put("limit", lm);
        sqlText.setSql(sql0);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("sortBy")).trim().isEmpty()) {
            String orderBy = UtCnv.toString(params.get("sortBy"));
            if (UtCnv.toBoolean(params.get("descending"))) {
                orderBy = orderBy + " desc";
            }
            sqlText = sqlText.replaceOrderBy(orderBy);
        }

        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("(cod like '%" + filter + "%' or f.name like '%" + filter + "%' or " +
                    "f.fullName like '%" + filter + "%')");
        Store st = mdb.createStore("Meter");
        mdb.loadQuery(st, sqlText, par);
        mdb.resolveDicts(st);

        return Map.of("store", st, "meta", meta);
    }

    /**
     * Delete Meter
     *
     * @param
     * @throws Exception
     */

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Meter
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
        Store st = mdb.createStore("Meter");
        mdb.loadQuery(st, "select * from Meter where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);

        //mdb.outTable(st);
        return st;
    }

    /**
     * Insert Meter
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("Meter");

        mdb.loadQuery(st, "select * from Meter where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        StoreRecord st = mdb.createStoreRecord("Meter");
        mdb.loadQueryRecord(st, "select * from Meter where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        DictService dictSvc = mdb.getModel().bean(DictService.class);
        Store st = mdb.createStore("Meter");
        StoreRecord rec = st.add();
        rec.set("accessLevel", FD_AccessLevel_consts.common);
        rec.set("meterStruct", FD_MeterStruct_consts.soft);
        rec.set("meterDeterm", FD_MeterDeterm_consts.determ);
        rec.set("distributionLaw", FD_DistributionLaw_consts.uniform);
        rec.set("meterTypeByRate", FD_MeterType_consts.integral);
        rec.set("meterTypeByPeriod", FD_MeterType_consts.integral);
        rec.set("meterTypeByMember", FD_MeterType_consts.integral);
        rec.set("meterBehavior", FD_MeterBehavior_consts.positive);
        dictSvc.resolveDicts(st);
        return rec;
    }

    public Store loadForSelect() throws Exception {
        Store st = mdb.createStore("Meter.select");
        return mdb.loadQuery(st, "select id, name, meterStruct, measure from Meter where 0=0");
    }


}

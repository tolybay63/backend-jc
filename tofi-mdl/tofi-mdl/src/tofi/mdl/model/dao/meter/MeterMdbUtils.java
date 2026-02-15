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
import tofi.mdl.model.utils.UtEntityTranslate;

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
     * @param params Map
     * @return Map["store"]["meta"]
     */
    public Map<String, Object> loadMeterPaginate(Map<String, Object> params) throws Exception {

        String lang = UtCnv.toString(params.get("lang"));
        String filter = UtCnv.toString(params.get("filter")).trim();

        //count
        String sqlCount = """
            select count(*) as cnt
            from Meter m
                left join TableLang l on l.nameTable='Meter' and m.id=l.idTable and
        """+"l.lang='"+lang+"' where 0=0";

        SqlText sqlText = mdb.createSqlText(sqlCount);
        sqlText.setSql(sqlCount);
        String textFilter = "name like '%" + filter + "%' or fullName like '%" + filter + "%' or cod like '%" + filter + "%' or cmt like '%" + filter + "%'";
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        int total = mdb.loadQuery(sqlText).get(0).getInt("cnt");
        int lm = UtCnv.toInt(params.get("rowsPerPage")) == 0 ? total : UtCnv.toInt(params.get("rowsPerPage"));
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", lm);

        //query
        String sqlLoad = """
            select *
            from Meter m
                left join TableLang l on l.nameTable='Meter' and m.id=l.idTable and l.lang=:lang
            where 0=0
            order by m.id
        """;

        sqlText = mdb.createSqlText(sqlLoad);

        int offset = (UtCnv.toInt(params.get("page")) - 1) * lm;
        sqlText.setSql(sqlLoad);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("sortBy")).trim().isEmpty()) {
            String orderBy = UtCnv.toString(params.get("sortBy"));
            if (UtCnv.toBoolean(params.get("descending"))) {
                orderBy = orderBy + " desc";
            }
            sqlText = sqlText.replaceOrderBy(orderBy);
        }

        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        //
        Store stLoad = mdb.createStore("Meter.lang");
        mdb.loadQuery(stLoad, sqlText, Map.of("lang", lang, "offset", offset, "limit", lm));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stLoad = ut.getTranslatedStore(stLoad,"Meter", lang);
        //
        return Map.of("store", stLoad, "meta", meta);
    }

    /**
     * Delete Meter
     *
     * @param rec Map
     */

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Meter
     *
     * @param params Map
     * @return Store
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
        return loadRec(Map.of("id", id, "lang", rec.get("lang")));
    }

    /**
     * Insert Meter
     *
     * @param params Map
     * @return  Store
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //
        return loadRec(Map.of("id", id, "lang", rec.get("lang")));
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        Store st = mdb.createStore("Meter.lang");
        mdb.loadQuery(st, "select * from Meter where id=:id", Map.of("id", id));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"Meter", lang);
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
        return rec;
    }

    public Store loadForSelect(String lang) throws Exception {
        Store st = mdb.createStore("Meter.select");
        mdb.loadQuery(st, "select id, meterStruct, measure from Meter where 0=0");
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Meter", lang);
    }


}

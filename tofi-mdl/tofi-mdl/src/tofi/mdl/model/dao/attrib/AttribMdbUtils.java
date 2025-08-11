package tofi.mdl.model.dao.attrib;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.HashMap;
import java.util.Map;

public class AttribMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public AttribMdbUtils(Mdb mdb, String tableName) {
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
     * Загрузка Attrib с пагинацией
     *
     * @param params Map
     * @return Map
     */
    public Map<String, Object> loadAttribPaginate(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo AuthUser
        long al = au.getAttrs().getLong("accesslevel");

        String sql = "select * from Attrib where accessLevel <= " + al + " order by id";
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
        Store st = mdb.createStore("Attrib");

        mdb.loadQuery(st, sqlText, par);
        mdb.resolveDicts(st);

        //count
        sql = "select count(*) as cnt from Attrib where accessLevel <= " + al;
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
     * Delete Attrib
     *
     * @param
     * @throws Exception
     */

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Factor & FactorVal
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
        Store st = mdb.createStore("Attrib");

        mdb.loadQuery(st, "select * from Attrib where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        //mdb.outTable(st);
        return st;
    }

    /**
     * Insert Attrib
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
        Store st = mdb.createStore("Attrib");
        mdb.loadQuery(st, "select * from Attrib where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);

        return st;
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        StoreRecord st = mdb.createStoreRecord("Attrib");
        mdb.loadQueryRecord(st, "select * from Attrib where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);

        return st;
    }

    public Store loadAttribChar(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("AttribChar");
        mdb.loadQuery(st, "select * from AttribChar where attrib=:attrib", params);
        mdb.resolveDicts(st);
        return st;
    }

    public Store insertAttribChar(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = mdb.insertRec("AttribChar", rec);
        //
        Store st = mdb.createStore("AttribChar");
        mdb.loadQuery(st, "select * from AttribChar where id=:id", Map.of("id", id));

        return st;
    }

    public Store updateAttribChar(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        mdb.updateRec("AttribChar", rec);
        //
        // Загрузка записи
        Store st = mdb.createStore("AttribChar");

        mdb.loadQuery(st, "select * from AttribChar where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);

        //mdb.outTable(st);
        return st;
    }

    public void deleteAttribChar(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("AttribChar", id);
    }


    public Store loadForSelect() throws Exception {
        Store st = mdb.createStore("Attrib");
        return mdb.loadQuery(st, "select * from Attrib where 0=0");
    }


}

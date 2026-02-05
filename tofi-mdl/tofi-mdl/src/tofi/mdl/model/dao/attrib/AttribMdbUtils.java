package tofi.mdl.model.dao.attrib;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.HashMap;
import java.util.Map;

public class AttribMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public AttribMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
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
        //mdb.resolveDicts(st);

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

    public Store loadAttrib(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");
        String whe = " accessLevel <= " + al;
        long id = UtCnv.toLong(params.get("id"));
        if (id > 0) {
            whe += " and id=" + id;
        }
        //
        Store st = mdb.createStore("Attrib.lang");
        String sql = "select * from Attrib where " + whe;
        sql += " order by id";
        mdb.loadQuery(st, sql);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"Attrib", lang);
    }


    /**
     *
     * @param rec Map
     * @throws Exception Exception
     */
    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Factor & FactorVal
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
        // Загрузка записи
        return loadAttrib(rec);
    }

    /**
     * Insert Attrib
     *
     * @param params Map
     * @return Store
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //
        rec.put("id", id);
        return loadAttrib(rec);
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

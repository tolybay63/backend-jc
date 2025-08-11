package tofi.mdl.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.HashMap;
import java.util.Map;


public class RoleMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;


    public RoleMdbUtils(Mdb mdb, String tableName) throws Exception {
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
     * Загрузка Role с пагинацией
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Map<String, Object> loadRolePaginate(Map<String, Object> params) throws Exception {

        String sql = "select * from role where 0=0 order by id";
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
        Store st = mdb.createStore("Role");
        mdb.loadQuery(st, sqlText, par);
        mdb.resolveDicts(st);

        //count
        sql = "select count(*) as cnt from role where 0=0";
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
     * Delete Role
     *
     * @param
     * @throws Exception
     */

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Role
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
        Store st = mdb.createStore("Role");

        mdb.loadQuery(st, "select * from Role where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        //mdb.outTable(st);
        return st;
    }

    /**
     * Insert Role
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
        Store st = mdb.createStore("Role");

        mdb.loadQuery(st, "select * from Role where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        StoreRecord st = mdb.createStoreRecord("Role");
        mdb.loadQueryRecord(st, "select * from Role where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public Store loadRoles(Map<String, Object> params) throws Exception {
        return mdb.loadQuery("select id, name from Role where 0=0");
    }


}

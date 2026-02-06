package tofi.mdl.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.Map;


public class RoleMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;


    public RoleMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }


    /**
     * Загрузка Role с пагинацией
     *
     * @param params Map
     * @return Store
     */
    public Store load(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("Role.lang");
        String whe = "0=0 order by id";
        long id = UtCnv.toLong(params.get("id"));
        if (id > 0) {
            whe = "id="+id + " order by id";
        }
        mdb.loadQuery(st, "select * from Role where "+whe);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"Role", lang);

    }

    /**
     * Delete Role
     *
     * @param rec Map
     */
    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Role
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
        return load(rec);
    }

    /**
     * Insert Role
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
        return load(rec);
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        StoreRecord st = mdb.createStoreRecord("Role");
        mdb.loadQueryRecord(st, "select * from Role where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public Store loadRoles(String lang) throws Exception {
        Store st = mdb.createStore("Role.lang");
        mdb.loadQuery(st, "select * from Role where 0=0 order by id");
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Role", lang);

    }


}

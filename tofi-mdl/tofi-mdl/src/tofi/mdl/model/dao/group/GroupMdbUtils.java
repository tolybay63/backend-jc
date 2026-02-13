package tofi.mdl.model.dao.group;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.Map;

public class GroupMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public GroupMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }


    public Store loadGroup(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore(tableName+".lang");
        mdb.loadQuery(st, "select * from " + tableName);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, tableName, UtCnv.toString(params.get("lang")));
    }

    public Store loadGroupForSelect(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong("id");
        Store st = mdb.createStore(tableName);
        mdb.loadQuery(st, "select * from " + tableName + " where id <> :id", Map.of("id", id));

        return st;
    }


    public Store loadRec(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore(tableName+".lang");
        mdb.loadQuery(st, "select * from " + tableName + " where id=" + UtCnv.toLong(params.get("id")));

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, tableName, UtCnv.toString(params.get("lang")));
    }

    public StoreRecord newRec() throws Exception {
        Store st = mdb.createStore(tableName);
        StoreRecord r = st.add();
        r.set("accessLevel", 1L);
        return r;
    }


    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = insertEntity(rec);
        //
        rec.put("id", id);
        return loadRec(rec);
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        // Загрузка записи
        Store st = mdb.createStore(tableName);
        mdb.loadQuery(st, "select * from " + tableName + " where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);

    }


}

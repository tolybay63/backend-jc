package tofi.mdl.model.dao.dimobj;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class DimObjMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public DimObjMdbUtils(Mdb mdb, String tableName) throws Exception {
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
     * @param dimObjGr
     * @return
     * @throws Exception
     */
    public Store loadDimObj(long dimObjGr) throws Exception {
        Store st = mdb.createStore("DimObj");
        return mdb.loadQuery(st, """
                    select * from DimObj where dimObjGr=:g
                    order by ord
                """, Map.of("g", dimObjGr));
    }

    /**
     * @param dimObjGroup
     * @return
     * @throws Exception
     */
    public StoreRecord newRec(long dimObjGroup) throws Exception {
        Store st = mdb.createStore("DimObj");
        StoreRecord r = st.add();
        r.set("dimObjGr", dimObjGroup);
        r.set("accessLevel", FD_AccessLevel_consts.common);
        return r;
    }

    /**
     * @param id
     * @return
     * @throws Exception
     */
    public StoreRecord loadRec(long id) throws Exception {
        Store st = mdb.createStore("DimObj");
        StoreRecord rec = st.add();
        return mdb.loadQueryRecord(rec, """
                    select * from DimObj where id=:id
                """, Map.of("id", id));
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("DimObj");
        mdb.loadQuery(st, "select * from DimObj where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

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
        Store st = mdb.createStore("DimObj");
        mdb.loadQuery(st, "select * from DimObj where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }


}

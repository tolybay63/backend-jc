package tofi.mdl.model.dao.syscoding;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_SysCodingType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.Map;

public class SysCodingMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public SysCodingMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }



    public Store load(String lang) throws Exception {
        Store st = mdb.createStore("SysCoding.lang");
        mdb.loadQuery(st, """
            select * from SysCoding where 0=0
        """);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "SysCoding", lang);
    }

    public StoreRecord newRec() {
        Store st = mdb.createStore("SysCoding");
        StoreRecord r = st.add();
        r.set("accessLevel", FD_AccessLevel_consts.common);
        r.set("sysCodingType", FD_SysCodingType_consts.reg);
        return  r;
    }

    private Store loadRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("SysCoding.lang");
        mdb.loadQuery(st, """
            select * from SysCoding where id=:id
        """, Map.of("id", id));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "SysCoding", lang);
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);
        //
        return loadRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        return loadRec(id, UtCnv.toString(rec.get("lang")));
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }


}

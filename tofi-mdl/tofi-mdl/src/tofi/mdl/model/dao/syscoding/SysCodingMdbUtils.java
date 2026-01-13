package tofi.mdl.model.dao.syscoding;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_SourceStockType_consts;
import tofi.mdl.consts.FD_SysCodingType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class SysCodingMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public SysCodingMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }



    public Store load() throws Exception {
        Store st = mdb.createStore("SysCoding");
        mdb.loadQuery(st, """
            select * from SysCoding where 0=0
        """);
        return st;
    }

    public StoreRecord newRec() {
        Store st = mdb.createStore("SysCoding");
        StoreRecord r = st.add();
        r.set("accessLevel", FD_AccessLevel_consts.common);
        r.set("sysCodingType", FD_SysCodingType_consts.reg);
        return  r;
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);

        Store st = mdb.createStore("SysCoding");
        mdb.loadQuery(st, "select * from SysCoding where id=:id", Map.of("id", id));
        return st;
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);

        Store st = mdb.createStore("SysCoding");
        mdb.loadQuery(st, "select * from SysCoding where id=:id", Map.of("id", id));
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }


}

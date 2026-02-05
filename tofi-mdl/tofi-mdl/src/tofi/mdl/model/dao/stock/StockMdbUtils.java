package tofi.mdl.model.dao.stock;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_SourceStockType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class StockMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public StockMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }



    public Store loadStocks(long stockGr) throws Exception {
        Store st = mdb.createStore("SourceStock");
        mdb.loadQuery(st, """
            select * from SourceStock where parent=:p
        """, Map.of("p", stockGr));
        return st;
    }

    public StoreRecord newRec(long stockGr) {
        Store st = mdb.createStore("SourceStock");
        StoreRecord r = st.add();
        r.set("parent", stockGr);
        r.set("accessLevel", FD_AccessLevel_consts.common);
        r.set("sourceStockType", FD_SourceStockType_consts.file);
        return  r;
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);

        Store st = mdb.createStore("SourceStock");
        mdb.loadQuery(st, "select * from SourceStock where id=:id", Map.of("id", id));
        return st;
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);

        Store st = mdb.createStore("SourceStock");
        mdb.loadQuery(st, "select * from SourceStock where id=:id", Map.of("id", id));
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    public Store loadStockForSelect() throws Exception {
        return mdb.loadQuery("""
            select id, name from SourceStock where 0=0
        """);
    }

}

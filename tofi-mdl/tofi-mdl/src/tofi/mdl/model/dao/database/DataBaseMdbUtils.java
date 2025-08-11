package tofi.mdl.model.dao.database;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_DataBaseType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class DataBaseMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public DataBaseMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public String getIdMetaModel() {
        CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
        return cfgSvc.getConf().getString("dbsource/default/id");
    }

    public Store load() throws Exception {
        Store st = mdb.createStore("DataBase");
        mdb.loadQuery(st, """
                    select * from DataBase where 0=0
                """);
        return st;
    }

    public StoreRecord newRec() {
        Store st = mdb.createStore("DataBase");
        return st.add();
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);

        Store st = mdb.createStore("DataBase");
        mdb.loadQuery(st, "select * from DataBase where id=:id", Map.of("id", id));
        return st;
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);

        Store st = mdb.createStore("DataBase");
        mdb.loadQuery(st, "select * from DataBase where id=:id", Map.of("id", id));
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    public Store loadDbForSelect() throws Exception {
        return mdb.loadQuery("""
                    select id, name from DataBase where dataBaseType=:db
                """, Map.of("db", FD_DataBaseType_consts.data));
    }

}

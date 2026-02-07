package tofi.mdl.model.dao.database;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_DataBaseType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

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

    public Store load(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DataBase.lang");
        String whe = "0=0 order by id";
        long id = UtCnv.toLong(params.get("id"));
        if (id > 0) {
            whe = "id="+id;
        }
        mdb.loadQuery(st, "select * from DataBase where "+whe);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"DataBase", lang);
    }

    public StoreRecord newRec() {
        Store st = mdb.createStore("DataBase");
        return st.add();
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);

        rec.put("id", id);
        return load(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        rec.put("id", id);
        return load(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    public Store loadDbForSelect(String lang) throws Exception {
        return mdb.loadQuery("""
            select d.id, l.name
            from database d, TableLang l
            where dataBaseType=:dbt and d.id=l.idTable and l.nameTable='DataBase' and l.lang=:lang
        """, Map.of("dbt", FD_DataBaseType_consts.data, "lang", lang));
    }

}

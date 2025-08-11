package tofi.mdl.model.dao.flat.table;

import jandcode.commons.UtCnv;
import jandcode.core.App;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import tofi.mdl.consts.FD_StorageType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class FlatTableMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public FlatTableMdbUtils(App app, Mdb mdb, String tableName) throws Exception {
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

    public Store loadTables(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("FlatTable");
        mdb.loadQuery(st, "select * from FlatTable where 0=0");
        //mdb.outTable(st);
        return st;
    }

    public Store load() throws Exception {
        Store st = mdb.createStore("FlatTable.full");
        mdb.loadQuery(st, """
                    select f.*, case when f.cls is null then rv.name else cv.name end as nameCls,
                        case when f.cls is null then r.dataBase else c.dataBase end as db,
                        case when f.cls is null then dr.name else dc.name end as nameDb
                    from FlatTable f
                        left join Cls c on f.cls=c.id
                        left join ClsVer cv on c.id=cv.ownerVer and cv.lastVer=1
                        left join DataBase dc on dc.id=c.dataBase
                        left join RelCls r on f.relCls=r.id
                        left join RelClsVer rv on r.id=rv.ownerVer and rv.lastVer=1
                        left join DataBase dr on dr.id=r.dataBase
                    where 0=0
                """);
        return st;
    }

    public Store loadRec(long id) throws Exception {
        Store st = mdb.createStore("FlatTable.full");
        mdb.loadQuery(st, """
                    select f.*, case when f.cls is null then rv.name else cv.name end as nameCls,
                        case when f.cls is null then r.dataBase else c.dataBase end as db,
                        case when f.cls is null then dr.name else dc.name end as nameDb
                    from FlatTable f
                        left join Cls c on f.cls=c.id
                        left join ClsVer cv on c.id=cv.ownerVer and cv.lastVer=1
                        left join DataBase dc on dc.id=c.dataBase
                        left join RelCls r on f.relCls=r.id
                        left join RelClsVer rv on r.id=rv.ownerVer and rv.lastVer=1
                        left join DataBase dr on dr.id=r.dataBase
                    where f.id=:id
                """, Map.of("id", id));
        return st;
    }

    public Store insertFlatTable(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);
        //
        return loadRec(id);
    }

    public Store updateFlatTable(Map<String, Object> rec) throws Exception {
        updateEntity(rec);
        //mdb.updateRec("FlatTable", rec);
        //
        Store st = mdb.createStore("FlatTable");
        return loadRec(UtCnv.toLong(rec.get("id"))); //  mdb.loadQuery(st, "select * from FlatTable where id=:id", Map.of("id",
        //rec.get("id")));
    }

    public void deleteFlatTable(Map<String, Object> rec) throws Exception {
        String tn = UtCnv.toString(rec.get("nameTable"));
        long id = UtCnv.toLong(rec.get("id"));

        try {
            mdb.execQuery("""
                        update TypCharGrProp set storageType=:sd, flatTable=null where flatTable=:ft;
                        update RelTypCharGrProp set storageType=:sd, flatTable=null where flatTable=:ft;
                    """, Map.of("sd", FD_StorageType_consts.std, "ft", id));
            deleteEntity(rec);
        } finally {
            //todo Можно убрать, они в другой базе
            mdb.execQuery("drop table if exists " + tn + " cascade");
            mdb.execQuery("drop table if exists " + tn + "_notuniqprop cascade");
        }
    }


}

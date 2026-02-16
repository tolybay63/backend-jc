package tofi.mdl.model.dao.flat.table;

import jandcode.commons.UtCnv;
import jandcode.core.App;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import tofi.mdl.consts.FD_StorageType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.Map;

public class FlatTableMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public FlatTableMdbUtils(App app, Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public Store loadTables(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("FlatTable");
        mdb.loadQuery(st, "select * from FlatTable where 0=0");
        //mdb.outTable(st);
        return st;
    }

    public Store load(long id, String lang) throws Exception {
        Store st = mdb.createStore("FlatTable.lang");
        String whe = "0=0";
        if (id >0)
            whe = "f.id="+id;
        mdb.loadQuery(st, """
            select f.id, f.cod, f.accesslevel, f.nametable, f.relcls, f.cls, l.name, l.fullName, l.cmt,
                case when f.cls is null then lrc.name else lc.name end as nameCls,
                case when f.cls is null then rc.dataBase else c.dataBase end as db,
                case when f.cls is null then ldrc.name else ldc.name end as nameDb
            from FlatTable f
                left join Cls c on f.cls=c.id
                left join ClsVer cv on c.id=cv.ownerVer and cv.lastVer=1
                left join RelCls rc on f.relcls=rc.id
                left join RelClsVer rcv on rc.id=rcv.ownerVer and rcv.lastVer=1
                left join DataBase dc on dc.id=c.database
                left join DataBase drc on drc.id=rc.dataBase
                left join TableLang lc on f.cls=lc.idTable and lc.nameTable='ClsVer' and lc.lang=:lang
                left join TableLang lrc on f.relcls=lrc.idTable and lrc.nameTable='RelClsVer' and lrc.lang=:lang
                left join TableLang ldc on dc.id=ldc.idTable and ldc.nameTable='DataBase' and ldc.lang=:lang
                left join TableLang ldrc on drc.id=ldrc.idTable and ldrc.nameTable='DataBase' and ldrc.lang=:lang
                left join TableLang l on f.id=l.idTable and l.nameTable='FlatTable' and l.lang=:lang
            where
        """+whe, Map.of("lang", lang));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "CubeS", lang);
    }

    public Store loadRec(long id) throws Exception {
        Store st = mdb.createStore("FlatTable.lang");
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
        return load(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateFlatTable(Map<String, Object> rec) throws Exception {
        updateEntity(rec);
        //
        return load(UtCnv.toLong(rec.get("id")), UtCnv.toString(rec.get("lang")));
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

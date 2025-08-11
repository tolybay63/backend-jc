package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class NotExtendedMdbUtils {
    Mdb mdb;

    public NotExtendedMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }


    public Store loadNotExtended(long typ) throws Exception {
        Store st = mdb.createStore("TypParentNot.full");

        String sql = """
                    select t.*, v.name as nameClass, d.modelName
                        --, ov.name as nameObj
                    from TypParentNot t
                        Left Join Cls c on t.clsOrObjCls=c.id
                        Left Join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                        Left Join DataBase d on d.id=c.database
                    where t.typ=:typ
                """;
        mdb.loadQuery(st, sql, Map.of("typ", typ));
        //
        //todo запрос для получения nameObj
        //

        return st;
    }

    protected void validNotExtended(StoreRecord r) throws Exception {
        long typ = r.getLong("typ");
        long cls = r.getLong("clsOrObjCls");
        long obj = r.getLong("obj");
        String sql = "";

        if (obj > 0) {
            Store st = mdb.loadQuery("""
                        select id from TypParentNot where typ=:typ and clsOrObjCls=:cls and obj=:obj
                    """, Map.of("typ", typ, "cls", cls, "obj", obj));

            if (st.size() > 0) {
                throw new XError("Данный объект уже указан");
            }
        } else {
            Store st = mdb.loadQuery("""
                        select id from TypParentNot where typ=:typ and clsOrObjCls=:cls
                    """, Map.of("typ", typ, "cls", cls));

            if (st.size() > 0) {
                throw new XError("Данный класс уже указан");
            }

        }

    }

    public Store insertNotExtended(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("TypParentNot");
        StoreRecord r = st.add(rec);
        validNotExtended(r);
        long id = mdb.insertRec("TypParentNot", r, true);
        //
        st = mdb.createStore("TypParentNot.full");
        mdb.loadQuery(st, """
                    select t.*, v.name as nameClass, d.modelName
                        --, ov.name as nameObj
                    from TypParentNot t
                        Left Join Cls c on t.clsOrObjCls=c.id
                        Left Join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                        Left Join DataBase d on d.id=c.database
                    where t.id=:id
                """, Map.of("id", id));
        //
        //todo запрос для получения nameObj
        //
        return st;
    }

    public Store updateNotExtended(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("TypParentNot");
        StoreRecord r = st.add(rec);
        validNotExtended(r);
        long id = r.getLong("id");
        mdb.updateRec("TypParentNot", r);
        //
        st = mdb.createStore("TypParentNot.full");
        mdb.loadQuery(st, """
                    select t.*, v.name as nameClass, d.modelName
                        --, ov.name as nameObj
                    from TypParentNot t
                        Left Join Cls c on t.clsOrObjCls=c.id
                        Left Join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                        Left Join DataBase d on d.id=c.database
                    where t.id=:id
                """, Map.of("id", id));
        //
        //todo запрос для получения nameObj
        //
        return st;
    }


    public void deleteNotExtended(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypParentNot", id);
    }
}

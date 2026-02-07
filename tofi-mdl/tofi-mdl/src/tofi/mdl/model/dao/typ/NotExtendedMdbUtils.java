package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.Map;

public class NotExtendedMdbUtils {
    Mdb mdb;

    public NotExtendedMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Store loadNotExtended(long id, long typ, String lang) throws Exception {
        Store st = mdb.createStore("TypParentNot.lang");
        String whe = "t.typ="+typ;
        if (id > 0)
            whe = "t.id="+id;

        String sql = """
                    select t.*, d.modelName
                    from TypParentNot t
                        Left Join Cls c on t.clsOrObjCls=c.id
                        Left Join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                        Left Join DataBase d on d.id=c.database
                    where :whe
                """;
        mdb.loadQuery(st, sql, Map.of("whe", whe));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"TypParentNot", lang, false);
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
        Store st = mdb.createStore("TypParentNot.lang");
        StoreRecord r = st.add(rec);
        validNotExtended(r);
        long id = mdb.insertRec("TypParentNot", r, true);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        r.set("id", id);
        ut.insertToTableLang(r.getValues());
        //
        return loadNotExtended(id, r.getLong("typ"), r.getString("lang"));
    }

    public Store updateNotExtended(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("TypParentNot.lang");
        StoreRecord r = st.add(rec);
        validNotExtended(r);
        long id = r.getLong("id");
        mdb.updateRec("TypParentNot", r);
        return loadNotExtended(id, r.getLong("typ"), r.getString("lang"));
    }

    public void deleteNotExtended(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypParentNot", id);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("TypParentNot", id);
    }

}

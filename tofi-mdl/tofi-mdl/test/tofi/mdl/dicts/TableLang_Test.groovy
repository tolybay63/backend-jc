package tofi.mdl.dicts

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class TableLang_Test extends Apx_Test {

    @Test
    void fill_DataBase() throws Exception { //1
        Store st = mdb.loadQuery("select * from DataBase where 0=0 order by id")
        for (StoreRecord r in st) {
            fill_TableLang("DataBase", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Measure() throws Exception { //2
        Store st = mdb.loadQuery("select * from Measure where 0=0 order by id")
        for (StoreRecord r in st) {
            fill_TableLang("Measure", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Attrib() throws Exception { //3
        Store st = mdb.loadQuery("select * from Attrib where 0=0 order by id")
        for (StoreRecord r in st) {
            fill_TableLang("Attrib", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Factor() throws Exception { //4
        Store st = mdb.loadQuery("select * from Factor order by id, parent")
        for (StoreRecord r in st) {
            fill_TableLang("Factor", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Meter() throws Exception { //5
        Store st = mdb.loadQuery("select * from Meter order by id")
        for (StoreRecord r in st) {
            fill_TableLang("Meter", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_MeterRate() throws Exception { //6
        Store st = mdb.loadQuery("select * from MeterRate order by id")
        for (StoreRecord r in st) {
            fill_TableLang("MeterRate", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    // 7 - Role

    @Test
    void fill_Typ() throws Exception { //8
        Store st = mdb.loadQuery("""
            select v.id, v.name, v.fullName, t.cmt 
            from Typ t, TypVer v
            where t.id=v.ownerVer and v.lastVer=1
            order by v.id
        """)
        for (StoreRecord r in st) {
            fill_TableLang("TypVer", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Cls() throws Exception { //9
        Store st = mdb.loadQuery("""
            select v.id, v.name, v.fullName, t.cmt 
            from Cls t, ClsVer v
            where t.id=v.ownerVer and v.lastVer=1
            order by v.id
        """)
        for (StoreRecord r in st) {
            fill_TableLang("ClsVer", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }


    @Test
    void fill_RelTyp() throws Exception { //10
        Store st = mdb.loadQuery("""
            select v.id, v.name, v.fullName, t.cmt 
            from RelTyp t, RelTypVer v
            where t.id=v.ownerVer and v.lastVer=1
            order by v.id
        """)
        for (StoreRecord r in st) {
            fill_TableLang("RelTypVer", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_RelTypMember() throws Exception { //11
        Store st = mdb.loadQuery("""
            select id, name, fullName, cmt 
            from RelTypMember
            where 0=0
            order by reltyp, ord
        """)
        for (StoreRecord r in st) {
            fill_TableLang("RelTypMember", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }


    @Test
    void fill_RelCls() throws Exception { //12
        Store st = mdb.loadQuery("""
            select v.id, v.name, v.fullName, t.cmt 
            from RelCls t, RelClsVer v
            where t.id=v.ownerVer and v.lastVer=1
            order by v.id
        """)

        for (StoreRecord r in st) {
            fill_TableLang("RelClsVer", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }


    @Test
    void fill_RelClsMember() throws Exception { //13
        Store st = mdb.loadQuery("""
            select id, name, fullName, cmt 
            from relclsmember
            where 0=0
            order by relcls, id
        """)

        for (StoreRecord r in st) {
            fill_TableLang("RelClsMember", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_PropGr() throws Exception { //14
        Store st = mdb.loadQuery("""
            select id, name, fullName, cmt from PropGr where 0=0 order by id
        """)

        for (StoreRecord r in st) {
            fill_TableLang("PropGr", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }


    //********************************************************************


    void fill_TableLang(String nameTable, long idTable,
                        String name, String fullName, String cmt, String lang) throws Exception {
        StoreRecord rec = mdb.createStoreRecord("TableLang")
        rec.set("nameTable", nameTable)
        rec.set("idTable", idTable)
        rec.set("name", name)
        rec.set("fullName", fullName)
        rec.set("cmt", cmt)
        rec.set("lang", lang)
        mdb.insertRec("TableLang", rec, true)
    }

}

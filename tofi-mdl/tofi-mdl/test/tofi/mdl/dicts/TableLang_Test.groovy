package tofi.mdl.dicts

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import tofi.mdl.model.utils.Translator

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

    //******************
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

/*
    @Test
    void update_RelClsVer() throws Exception { //12
        Store st = mdb.loadQuery("""
            select * from tablelang where nameTable='RelClsVer' and lang='ru'
            order by id
        """)
        for (StoreRecord r in st) {
            def arr = r.getString("name").split("<=>")
            String nm = arr[0].trim() + " \u21D4 " + arr[1].trim()
            mdb.execQuery("""
                update TableLang set name='${nm}', fullName='${nm}' where id=${r.getLong("id")}
            """)
        }

    }
*/

    //******************

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

    @Test
    void fill_Prop() throws Exception { //15
        Store st = mdb.loadQuery("""
            select id, name, fullName, cmt from Prop where 0=0 order by id
        """)

        for (StoreRecord r in st) {
            fill_TableLang("Prop", r.getLong("id"), r.getString("name"),
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


    //********************************************************************

    @Test
    void test_TranslateFactor() {
        //translateTable("Factor", "kk")
        translateTable("Prop", "kk")
    }



    void translateTable(String table, String lang) throws Exception {
        Translator tr = new Translator(mdb)
        Store st = mdb.createStore("TableLang")
/*
        mdb.loadQuery(st, """
            select * from TableLang where nameTable='${table}' and lang='ru';
        """)
*/

        mdb.loadQuery(st, """            
            select *
            from tablelang
            where nameTable='Prop' and lang='ru' and 
            idtable not in (
                select idTable
                from tablelang
                where nameTable='Prop' and lang='kk'
            )
        """)

        for (StoreRecord rec in st) {
            String nm, fn, cmt = null
            nm = tr.translateText(rec.getString("name"), "ru", lang)
            fn = nm
            if (!rec.getString("fullName").isEmpty() && rec.getString("name") != rec.getString("fullName"))
                fn = tr.translateText(rec.getString("fullName"), "ru", lang)


            if (!rec.getString("cmt").isEmpty())
                cmt = tr.translateText(rec.getString("cmt"), "ru", lang)

            StoreRecord rr = mdb.createStoreRecord("TableLang", rec)
            rr.set("id", null)
            rr.set("name", nm)
            rr.set("fullName", fn)
            rr.set("cmt", cmt)
            rr.set("lang", lang)
            mdb.insertRec("TableLang", rr, true)
        }
    }


}

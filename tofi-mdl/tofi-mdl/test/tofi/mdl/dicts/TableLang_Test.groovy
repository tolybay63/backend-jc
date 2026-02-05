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
        Store st = mdb.loadQuery("select * from Measure where 0=0 order by id limit 5")
        for (StoreRecord r in st) {
            fill_TableLang("Measure", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Attrib() throws Exception { //2
        Store st = mdb.loadQuery("select * from Attrib where 0=0 order by id")
        for (StoreRecord r in st) {
            fill_TableLang("Attrib", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }

    @Test
    void fill_Factor() throws Exception { //2
        Store st = mdb.loadQuery("select * from Factor where parent is null order by ord limit 10")
        for (StoreRecord r in st) {
            fill_TableLang("Factor", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }

        st = mdb.loadQuery("""
            select * from Factor where parent in (
                select id from Factor where parent is null limit 10            
            ) order by ord
        """)
        for (StoreRecord r in st) {
            fill_TableLang("Factor", r.getLong("id"), r.getString("name"),
                    r.getString("fullName"), r.getString("cmt"), "ru")
        }
    }


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

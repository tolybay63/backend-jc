package tofi.mdl.dicts

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class TableLang_Test extends Apx_Test {

    @Test
    void fill_DataBase() throws Exception {
        Store st = mdb.loadQuery("select * from DataBase where 0=0 order by id")
        for (StoreRecord r in st) {
            fill_TableLang("DataBase", r.getLong("id"), r.getString("name"),
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

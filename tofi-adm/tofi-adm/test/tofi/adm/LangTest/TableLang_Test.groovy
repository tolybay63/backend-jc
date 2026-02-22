package tofi.adm.LangTest

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import tofi.adm.model.utils.Translator

class TableLang_Test extends Apx_Test {


    @Test
    void copyTable() {
        //1
        //copy2TableLang("AuthRole")
        //2
        //copy2TableLang("AuthUser")
        //3
        copy2TableLang("AuthUserGr")

    }



    @Test
    void test_Translate() {
        //1
        //translateTable("AuthRole", "kk")
        //translateTable("AuthRole", "en-US")
        //2
        //translateTable("AuthUser", "kk")
        //translateTable("AuthUser", "en-US")
        //3
        //translateTable("AuthUserGr", "kk")
        translateTable("AuthUserGr", "en-US")

    }


    /* ************************************************************** */
    private void copy2TableLang(String table) throws Exception {
        Store st0 = mdb.createStore(table+".lang")
        mdb.loadQuery(st0, "select * from ${table} where 0=0 order by id")
        for (StoreRecord r in st0) {
            StoreRecord rec = mdb.createStoreRecord("TableLang")
            rec.set("nameTable", table)
            rec.set("idTable", r.getLong("id"))
            rec.set("name", r.getString("name"))
            rec.set("lang", "ru")
            if (r.findField("fullName")) {
                if (!r.getString("fullName").isEmpty())
                    rec.set("fullName", r.getString("fullName"))
            }
            if (r.findField("cmt")) {
                if (!r.getString("cmt").isEmpty())
                    rec.set("cmt", r.getString("cmt"))
            }
            mdb.insertRec("TableLang", rec, true)
        }
    }

    private void translateTable(String table, String lang) throws Exception {
        Translator tr = new Translator(mdb)
        Store st = mdb.createStore("TableLang")
        mdb.loadQuery(st, """
            select * from TableLang where nameTable='${table}' and lang='ru' order by id;
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

package tofi.adm

import jandcode.commons.UtString
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import tofi.adm.model.dao.permis.PermisMdbUtils
import tofi.adm.model.utils.Translator

class PermisTest extends Apx_Test {

    @Test
    void testIsLeaf() throws Exception {
        PermisMdbUtils utils = new PermisMdbUtils(mdb)
        Set<String> set = utils.getLeaf("nsi:collection:ins")
        System.out.println(UtString.join(set, "; "))
    }


    @Test
    void copyPermis2TableLang() throws Exception {
        Store st0 = mdb.createStore("Permis.lang")
        mdb.loadQuery(st0, "select * from Permis where 0=0 order by id")
        for (StoreRecord r in st0) {
            StoreRecord rec = mdb.createStoreRecord("TableLang")
            rec.set("nameTable", "Permis")
            rec.set("name", r.getString("id"))
            rec.set("fullName", r.getString("name"))
            rec.set("lang", "ru")
            mdb.insertRec("TableLang", rec, true)
        }
    }

    @Test
    void translate() {
        translatePermis("en-US")
    }


    private void translatePermis(String lang) throws Exception {
        Translator tr = new Translator(mdb)
        Store st = mdb.createStore("TableLang")
        mdb.loadQuery(st, """
            select * from TableLang where nameTable='Permis' and lang='ru' order by id;
        """)

        for (StoreRecord rec in st) {
            String fn
            fn = tr.translateText(rec.getString("fullName"), "ru", lang)

            StoreRecord rr = mdb.createStoreRecord("TableLang", rec)
            rr.set("id", null)
            rr.set("fullName", fn)
            rr.set("lang", lang)
            mdb.insertRec("TableLang", rr, true)
        }
    }



}

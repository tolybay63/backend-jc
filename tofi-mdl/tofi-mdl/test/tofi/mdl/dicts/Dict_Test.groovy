package tofi.mdl.dicts

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import tofi.mdl.model.utils.Translator


class Dict_Test extends Apx_Test {

    @Test
    void test_GenDictLang() throws Exception {

        Store stDicts = mdb.loadQuery("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' and table_name like 'fd_%' and table_name != 'fd_lang' and table_name != 'fd_dictslang'
            order by table_name
        """)

        Translator tr = new Translator(mdb)
        long id = 1
        for (StoreRecord r : stDicts) {
            Store st = mdb.loadQuery("select * from ${r.getString('table_name')} order by ord")
            for (String lg in ["ru"]) {
                for (StoreRecord rr : st) {
                    println("""<row id="${id}" idDict="${rr.getLong("id")}" nameDict="${r.getString("table_name")}" lang="${lg}" text="${rr.getString("text")}"/>""")
                    id++
                }
            }
            for (String lg in ["kk", "en-US"]) {
                for (StoreRecord rr : st) {
                    String text = tr.translateText(rr.getString("text"), "ru", lg)

                    println("""<row id="${id}" idDict="${rr.getLong("id")}" nameDict="${r.getString("table_name")}" lang="${lg}" text="${text}"/>""")
                    id++
                }
            }

            println()
        }
    }

    @Test
    void test_TranslateText() {

        Translator tr = new Translator(dbm.mdb)

        System.out.println("Привет!")

        String textRu = tr.translateText("Привет!", "ru", "en-US")
        System.out.println("enUS: " + textRu)

        String textKk = tr.translateText("Привет!", "ru", "kk")
        System.out.println("kk: " + textKk)

        String textTur = tr.translateText("Привет!", "ru", "tr")
        System.out.println("tr: " + textTur)
    }

}

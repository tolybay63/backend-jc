package tofi.mdl.dicts

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test


class Dict_Test extends Apx_Test {

    @Test
    void test_GenDictLang() throws Exception {

        Store stDicts = mdb.loadQuery("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' and table_name like 'fd_%' and table_name != 'fd_lang'
            order by table_name
        """)

        long id = 1
        for (StoreRecord r : stDicts) {
            Store st = mdb.loadQuery("select * from ${r.getString('table_name')} order by ord")
            for (String lg in ["ru", "kk", "en-US"]) {
                for (StoreRecord rr : st) {
                    println("""<row id="${id}" idDict="${rr.getLong("id")}" nameDict="${r.getString("table_name")}" lang="${lg}" text="${rr.getString("text")}"/>""")
                    id++
                }
            }
            println()
        }
    }

}

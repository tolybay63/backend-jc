package tofi.mdl.typ.Syscod

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class Syscod_Test extends Apx_Test {

    @Test
    void test1() {
        Store st = dbm.mdb.loadQuery("""
            with sc as (
                select id  
                from DimMultiPropItem
                except
                select linkid as id 
                from syscod
                where cod like '_DMPI%'
            )
            select id, cod  
            from DimMultiPropItem
            where id in (select id from sc)
            order by ord
        """)

        for (StoreRecord r in st) {
            StoreRecord recSC = dbm.mdb.createStoreRecord("SysCod")
            recSC.set("linkType", 17L)
            recSC.set("linkId", r.getLong("id"))
            recSC.set("cod", "_DMPI_"+r.getString("id"))
            dbm.mdb.insertRec("SysCod", recSC, true)
        }
    }

}

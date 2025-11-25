package tofi.mdl.typ.data

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class Data_Test extends Apx_Test {

    @Test
    void test1() {
        String clsORrelcls = "cls"

        Map<Long, String> res = new HashMap<>()
        Store st = getMdb().loadQuery("""
            select ${clsORrelcls}, string_agg (cast(id as varchar(3000)), ',' order by id) as lst
            from PropVal
            where ${clsORrelcls} is not null
            group by ${clsORrelcls}
        """)

        for (StoreRecord r : st) {
            res.put(r.getLong(clsORrelcls), r.getString("lst"))
        }

        getMdb().outMap(res)

    }

}

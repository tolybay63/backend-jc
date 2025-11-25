package tofi.mdl.typ.data;

import jandcode.core.apx.test.Apx_Test;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class Java_Test extends Apx_Test {

    @Test
    public void test() throws Exception {
        String clsORrelcls = "cls";
        Map<Long, String> res = new HashMap<>();

        Store st = getMdb().loadQuery(String.format("""
            select %s, string_agg (cast(id as varchar(3000)), ',' order by id) as lst
            from PropVal
            where %s is not null
            group by %s
        """, clsORrelcls, clsORrelcls, clsORrelcls));

        for (StoreRecord r : st) {
            res.put(r.getLong(clsORrelcls), r.getString("lst"));
        }

        getMdb().outMap(res);
    }


}

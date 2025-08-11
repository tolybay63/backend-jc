package example.apinator1.dao;

import jandcode.core.dbm.mdb.*;
import jandcode.core.store.*;

import java.util.*;

public class Example2Java_dao extends BaseMdbUtils {

    /**
     * Демонстрация метода withMdb на java
     */
    public List<String> getFactorNamesDirect() throws Exception {
        List<String> res = new ArrayList<>();

        getMdb().withMdb("meta", mdb1 -> {
            Store st = mdb1.loadQuery("""
                    select * from factor
                    where parent is null
                    order by id""");
            for (var r : st) {
                res.add(r.getString("name"));
            }
        });

        return res;
    }


}

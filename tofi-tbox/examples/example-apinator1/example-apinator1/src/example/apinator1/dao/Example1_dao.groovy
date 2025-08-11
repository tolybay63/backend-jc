package example.apinator1.dao

import jandcode.core.dbm.mdb.*
import jandcode.core.store.*
import tofi.api.adm.*
import tofi.api.mdl.*
import tofi.apinator.*

/**
 * Пример dao
 */
class Example1_dao extends BaseMdbUtils {

    protected ApinatorApi apiAdm() {
        return app.bean(ApinatorService).getApi("adm")
    }

    protected ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    List<String> getUserNames() {
        ExampleApiUser z = apiAdm().get(ExampleApiUser)
        Store st = z.getAllUsersAsStore()
        //
        List<String> res = new ArrayList<>()
        for (r in st) {
            res.add(r.getString("login"))
        }
        return res
    }

    List<String> getFactorNames() {
        ExampleApiMeta z = apiMeta().get(ExampleApiMeta)
        Store st = z.getAllFactors()
        //
        List<String> res = new ArrayList<>()
        for (r in st) {
            res.add(r.getString("name"))
        }
        return res
    }

    /**
     * Демонстрация метода withMdb
     */
    List<String> getFactorNamesDirect() {
        List<String> res = new ArrayList<>()

        mdb.withMdb("meta", { mdb1 ->
            Store st = mdb1.loadQuery("""
                select * from factor
                where parent is null
                order by id
            """)
            for (r in st) {
                res.add(r.getString("name"))
            }
        })

        return res
    }


}

package tofi.api.mdl.impl

import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.mdl.ExampleApiMeta


class ExampleApiMetaImpl extends BaseMdbUtils implements ExampleApiMeta{

    Store getAllFactors() {
        Store st = mdb.loadQuery("""
            select * from factor
            where parent is null
            order by id
        """)
        return st
    }

}

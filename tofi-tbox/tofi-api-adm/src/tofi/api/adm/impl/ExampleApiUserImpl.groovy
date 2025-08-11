package tofi.api.adm.impl

import jandcode.core.dbm.mdb.*
import jandcode.core.store.*
import tofi.api.adm.*

class ExampleApiUserImpl extends BaseMdbUtils implements ExampleApiUser {

    Store getAllUsersAsStore() {
        Store st = mdb.loadQuery("""
            select * from authuser order by id
        """)
        return st
    }

}

package tofi.adm

import jandcode.commons.UtString
import jandcode.core.apx.test.Apx_Test
import org.junit.jupiter.api.Test
import tofi.adm.model.dao.permis.PermisMdbUtils

class PermisTest extends Apx_Test {

    @Test
    void testIsLeaf() throws Exception {
        PermisMdbUtils utils = new PermisMdbUtils(mdb)
        Set<String> set = utils.getLeaf("nsi:collection:ins")
        System.out.println(UtString.join(set, "; "))
    }


}

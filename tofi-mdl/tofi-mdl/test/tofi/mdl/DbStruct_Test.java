package tofi.mdl;

import jandcode.core.apx.test.Apx_Test;
import jandcode.core.dbm.ddl.DDLScript;
import jandcode.core.dbm.ddl.DDLService;
import org.junit.jupiter.api.Test;

public class DbStruct_Test extends Apx_Test {

    /**
     * Получение полного скрипта для создания базы данных с нуля.
     */
    @Test
    public void generate_create_sql() throws Exception {
        DDLService svc = getModel().bean(DDLService.class);
        DDLScript script = svc.grabScript();
        String text = script.getSqlScript();
        System.out.println(text);
    }


}

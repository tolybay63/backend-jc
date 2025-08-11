package tofi.mdl;

import jandcode.core.apx.test.Apx_Test;
import jandcode.core.dbm.ModelDef;
import jandcode.core.dbm.ModelService;
import org.junit.jupiter.api.Test;

public class Model_Test extends Apx_Test {

    /**
     * Список всех модлей
     */
    @Test
    public void model_list() throws Exception {
        ModelService modelSvc = app.bean(ModelService.class);
        for (ModelDef modelDef : modelSvc.getModels()) {
            System.out.println(modelDef.getName() + " : instance? " + modelDef.isInstance());
            if (modelDef.isInstance()) {
                System.out.println("  instanceof : " + modelDef.getInstanceOf().getName());
            }

        }
    }


}

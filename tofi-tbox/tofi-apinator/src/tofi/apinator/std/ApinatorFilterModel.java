package tofi.apinator.std;

import jandcode.commons.error.*;
import jandcode.core.db.*;
import jandcode.core.dbm.*;
import jandcode.core.dbm.impl.*;
import jandcode.core.dbm.mdb.*;
import tofi.apinator.*;

public class ApinatorFilterModel implements ApinatorFilter {

    public void execApinatorFilter(ApinatorFilterType type, ApinatorContext ctx) throws Exception {
        if (type == ApinatorFilterType.before) {
            before(ctx);
        } else if (type == ApinatorFilterType.after) {
            after(ctx);
        } else if (type == ApinatorFilterType.cleanup) {
            cleanup(ctx);
        }
    }

    protected void before(ApinatorContext ctx) throws Exception {
        String modelName = ctx.getConf().getString("model", "default");
        Model model = ctx.getApp().bean(ModelService.class).getModel(modelName);

        //
        Db db = model.createDb();
        Db dbw = new ModelDbWrapper(db, true, true);
        Mdb mdb = model.createMdb(dbw);

        //
        ctx.getBeanFactory().registerBean(Db.class.getName(), dbw);
        ctx.getBeanFactory().registerBean(Model.class.getName(), model);
        ctx.getBeanFactory().registerBean(Mdb.class.getName(), mdb);

        // инициализация экземпляра
        Object inst = ctx.getInst();
        if (inst instanceof IMdbLinkSet) {
            ((IMdbLinkSet) inst).setMdb(mdb);
        }
        if (mdb instanceof IValidateErrorsLinkSet) {
            ((IValidateErrorsLinkSet) mdb).setValidateErrors(ctx.bean(ValidateErrors.class));
        }
    }

    protected void after(ApinatorContext ctx) throws Exception {
        Db db = ctx.bean(Db.class);
        if (db.isConnected()) {
            // кто то попользовался
            if (db.isTran()) {
                db.commit();
            }
            db.disconnectForce();
        }
    }

    protected void cleanup(ApinatorContext ctx) throws Exception {
        Db db = ctx.bean(Db.class);
        if (db.isConnected()) {
            // кто то попользовался и не закрыл
            // например в after не сработал commit
            try {
                if (db.isTran()) {
                    db.rollback();
                }
            } finally {
                db.disconnectForce();
            }
        }
    }

}

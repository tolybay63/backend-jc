package dtj.nsi.action;

import dtj.nsi.dao.FillDao;
import jandcode.commons.UtCnv;
import jandcode.core.dbm.ModelService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.web.HttpError;
import jandcode.core.web.action.BaseAction;


import java.io.File;
import java.util.Objects;

public class FillDataAction extends BaseAction {

    protected void onExec() throws Exception {

        String tempDir = UtCnv.toString(getReq().getHttpServlet().getServletContext().getAttribute("javax.servlet.context.tempdir"));
        if (tempDir==null) {
            throw new HttpError(404);
        }

        String fnOrg = getReq().getParams().getString("filename");
        boolean fill = getReq().getParams().getBoolean("fill");

        //Сгенерированный файл
        File fle = findFile(tempDir);

        if (fle != null) {
            ModelService modelSvc = getApp().bean(ModelService.class);
            Mdb mdb =  modelSvc.getModel().createMdb();
            FillDao dao = mdb.createDao(FillDao.class);
            dao.fillObjectService(fle, fill);
        }

        getReq().render("filename: "+ fnOrg);

    }

    private File findFile(String path) throws Exception {
        for(File item : Objects.requireNonNull(new File(path).listFiles())){
            if (!item.isDirectory()){
                if (item.getName().startsWith("undertow") &&
                        item.getName().endsWith("upload")) {
                    return item;
                }
            }
        }
        return null;
    }


}

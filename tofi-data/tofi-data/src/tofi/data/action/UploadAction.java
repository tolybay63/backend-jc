package tofi.data.action;

import jandcode.commons.UtCnv;
import jandcode.commons.UtJson;
import jandcode.commons.conf.Conf;
import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.commons.variant.VariantMap;
import jandcode.core.dbm.ModelService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.DataDirService;
import jandcode.core.web.HttpError;
import jandcode.core.web.action.BaseAction;
import tofi.api.mdl.ApiMeta;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;
import tofi.data.model.DataDao;
import java.io.File;
import java.util.Objects;

public class UploadAction extends BaseAction {

    ApinatorApi apiMeta() {
        return getApp().bean(ApinatorService.class).getApi("meta");
    }


    protected void onExec() throws Exception {

        String tempDir = UtCnv.toString(getReq().getHttpServlet().getServletContext().getAttribute("javax.servlet.context.tempdir"));
        if (tempDir==null) {
            throw new HttpError(404);
        }

        //Оригинальное имя файла

        IVariantMap params = UtJson.fromJson(getReq().getParams().getString("params"), VariantMap.class);
        String fnOrg = params.getString("filename");

        //Сгенирированный файл
        File fle = findFile(tempDir);

        if (fle != null) {

            params.put("filePath", fle.getAbsolutePath());
            String path = "";
            try {
                path = getApp().bean(DataDirService.class).getPath("dbfilestorage");
            } catch (Exception e) {
                path = "";
            }

            String bucketName = "";
            try {
                Conf conf2 = getApp().getConf().getConf("datadir/minio");
                bucketName = conf2.getString("bucketName");
            } catch (Exception e) {
                bucketName = "";
            }

            if (!path.isEmpty() && bucketName.isEmpty()) {
                uploadFS(fle, params);
            } else if (path.isEmpty() && !bucketName.isEmpty()) {
                uploadMinio(params);
            } else {
                throw new XError("FileStorage не настроен!");
            }
        } else {
            throw  new XError("File not found");
        }
        //
        getReq().render("FileName: " + fnOrg);

    }

    private void uploadMinio(IVariantMap params) throws Exception {

    }

    private void uploadFS(File fle, IVariantMap params) throws Exception {
        String fnOrg = params.getString("filename");
        ModelService modelSvc = getApp().bean(ModelService.class);
        Mdb mdb =  modelSvc.getModel().createMdb();
        DataDao daoVal = mdb.createDao(DataDao.class);
        try {
            DbFileStorageService fsService = apiMeta().get(ApiMeta.class).getDbFileStorageService();
            fsService.setModelName(params.getString("model"));
            DbFileStorageItem dfi = fsService.addFile(fle, fnOrg);
            long idFile = dfi.getId();
            //
            try {
                params.put("fileVal", idFile);
                daoVal.insertAttribValue(params);
            } catch (Exception e) {
                fsService.removeFile(idFile);
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private File findFile(String path) throws Exception {
        File dir = new File(path);
        for(File item : Objects.requireNonNull(dir.listFiles())){
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

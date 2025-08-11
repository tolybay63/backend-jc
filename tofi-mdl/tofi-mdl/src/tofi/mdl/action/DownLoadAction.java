package tofi.mdl.action;

import jandcode.commons.UtCnv;
import jandcode.commons.UtFile;
import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.core.web.HttpError;
import jandcode.core.web.WebService;
import jandcode.core.web.action.BaseAction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

public class DownLoadAction extends BaseAction {

    protected void onExec() throws Exception {

        String tempDir = UtCnv.toString(getReq().getHttpServlet().getServletContext().getAttribute("javax.servlet.context.tempdir"));
        if (tempDir == null) {
            throw new HttpError(404);
        }
        WebService svc = getReq().getWebService();

        //Оригинальное имя файла
        //IVariantMap params = UtJson.fromJson(getReq().getParams().getString("params"), VariantMap.class);
        IVariantMap params = getReq().getParams();
        String fnOrg = params.getString("filename");
        //Сгенирированный файл
        File fle = findFile(tempDir);

        if (fle != null) {

            //TimeUnit.SECONDS.sleep(20);

            Map<String, String> mapEnv = System.getenv();
            String download_dir = mapEnv.get("USERPROFILE") + File.separator + "Downloads";

            File file = new File(UtFile.join(download_dir, fnOrg));
            if (UtFile.exists(file)) {
                file.delete();
            }

            try (InputStream in = new FileInputStream(fle)) {
                Files.copy(in, Paths.get(download_dir, fnOrg));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                getReq().render("FileName: " + fnOrg);
            }

/*
            ModelService modelSvc = getApp().bean(ModelService.class);
            Mdb mdb =  modelSvc.getModel().createMdb();
            FileDao fileDao = mdb.createDao(FileDao.class);

            FileWrapper outputStream =  fileDao.download(fle.getAbsolutePath());
*/


        } else {
            throw new XError("File not found");
        }
    }

    private File findFile(String path) throws Exception {
        File dir = new File(path);
        for (File item : Objects.requireNonNull(dir.listFiles())) {
            if (!item.isDirectory()) {
                if (item.getName().startsWith("undertow") &&
                        item.getName().endsWith("upload")) {
                    return item;
                }
            }
        }
        return null;
    }


}

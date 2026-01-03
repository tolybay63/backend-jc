package dtj.inspection.action;

import dtj.inspection.dao.ImportXmlDao;
import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import jandcode.core.web.HttpError;
import jandcode.core.web.action.BaseAction;
import tofi.api.dta.ApiInspectionData;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ImportAction extends BaseAction {

    ApinatorApi apiInspection() {
        return getApp().bean(ApinatorService.class).getApi("inspectiondata");
    }

    protected void onExec() throws Exception {

        String tempDir = UtCnv.toString(getReq().getHttpServlet().getServletContext().getAttribute("javax.servlet.context.tempdir"));
        if (tempDir == null) {
            throw new HttpError(404);
        }

        //Оригинальное имя файла
        IVariantMap params = getReq().getParams();
        String filename = params.getString("filename");

        //Сгенирированный файл
        File fle = findFile(tempDir);
        Store st = null;

        if (fle != null) {
            Mdb mdb = apiInspection().get(ApiInspectionData.class).getMdbForImport();
            ImportXmlDao importXmlDao = mdb.createDao(ImportXmlDao.class);
            st = importXmlDao.analyze(fle, params);
        } else {
            throw new XError("File not found");
        }
        //
        //getReq().render("FileName: " + filename);
        List<Map<String, Object>> res = new ArrayList<>();
        for (StoreRecord r : st.getRecords()) {
            res.add(r.getValues());
        }
        getReq().render(res);

    }

    private File findFile(String path) throws Exception {
        File dir = new File(path);
        for (File item : Objects.requireNonNull(dir.listFiles())) {
            if (!item.isDirectory()) {
                if (item.getName().startsWith("undertow") && item.getName().endsWith("upload"))
                    return item;
            }
        }
        return null;
    }

}

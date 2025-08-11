package dtj.inspection.action;

import jandcode.commons.variant.IVariantMap;
import jandcode.core.web.HttpError;
import jandcode.core.web.action.BaseAction;
import tofi.api.mdl.ApiMeta;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;

import java.io.File;

public class DownloadAction extends BaseAction {

    ApinatorApi apiMeta() {
        return getApp().bean(ApinatorService.class).getApi("meta");
    }

    protected void onExec() throws Exception {
        IVariantMap params = getReq().getParams();

        DbFileStorageService dfsrv = apiMeta().get(ApiMeta.class).getDbFileStorageService();
        dfsrv.setModelName(params.getString("file_dir"));
        DbFileStorageItem fi = dfsrv.getFile(params.getLong("id"));
        File fs;
        String fn;
        try {
             fs = fi.getFile();
             fn = fi.getOriginalFilename();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new HttpError(404);
        }

        var res = new DownFile(fs, fn);

        getReq().render(res);
    }


}

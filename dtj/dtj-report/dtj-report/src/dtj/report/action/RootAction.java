package dtj.report.action;

import jandcode.core.web.HttpError;
import jandcode.core.web.WebService;
import jandcode.core.web.action.BaseAction;
import jandcode.core.web.virtfile.VirtFile;

public class RootAction extends BaseAction {

    protected void onExec() throws Exception {
        WebService svc = getReq().getWebService();

        VirtFile f = svc.findFile("index.html");
        if (f==null) {
            throw new HttpError(404);
        }
        getReq().render(f);
    }
}

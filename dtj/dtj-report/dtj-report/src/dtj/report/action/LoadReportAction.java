package dtj.report.action;

import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.core.web.action.BaseAction;

import java.io.File;


public class LoadReportAction extends BaseAction {

    protected void onExec() throws Exception {
        IVariantMap params = getReq().getParams();
        String fn, fon;
        if (!params.getString("id").isEmpty()) {
            if (params.getString("ext").equalsIgnoreCase("pdf")) {
                fn = getApp().getAppdir() + File.separator + "reports" + File.separator + params.getString("id") + ".pdf";
                if (params.getString("tml").equalsIgnoreCase("по-4")) {
                    fon = "ПО-4.pdf";
                } else if (params.getString("tml").equalsIgnoreCase("по-6")) {
                    fon = "ПО-6.pdf";
                } else {
                    throw new XError("Not found [tml]");
                }
            } else {
                fn = getApp().getAppdir() + File.separator + "reports" + File.separator + params.getString("id") + ".xlsx";
                if (params.getString("tml").equalsIgnoreCase("по-4")) {
                    fon = "ПО-4.xlsx";
                } else if (params.getString("tml").equalsIgnoreCase("по-6")) {
                    fon = "ПО-6.xlsx";
                } else {
                    throw new XError("Not found [tml]");
                }
            }
        } else {
            throw new XError("Not found [id]");
        }

        File fs = new File(fn);
        DownFile res = new DownFile(fs, fon);
        getReq().render(res);
    }

}

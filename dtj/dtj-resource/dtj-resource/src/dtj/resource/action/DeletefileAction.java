package dtj.resource.action;

import jandcode.commons.UtFile;
import jandcode.core.web.action.BaseAction;

import java.io.File;

public class DeletefileAction extends BaseAction {


    protected void onExec() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir");

        String fn = getReq().getParams().getString("filename");
        fn = fn.replace(UtFile.ext(fn), "pdf");
        String destPath = tempDir + fn;
        File file = new File(destPath);
        if (file.delete())
            getReq().render("Ok");
        else
            getReq().render("Error");
    }
}

package dtj.inspection.action;

import jandcode.commons.UtFile;
import jandcode.core.web.UtWeb;
import jandcode.core.web.render.BaseRender;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class DownFileRender extends BaseRender {

    @Override
    protected void onRender(Object data) throws Exception {
        DownFile md = (DownFile) data;

        UtWeb.setHeaderDownload(getRequest(), md.getClientName(), md.getFile().length());

        InputStream stmSource = new FileInputStream(md.getFile());
        OutputStream stmDest = getRequest().getOutStream();
        try {
            UtFile.copyStream(stmSource, stmDest);
        } finally {
            stmSource.close();
        }
    }

}

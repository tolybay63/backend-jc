package dtj.nsi.action;

import com.documents4j.api.DocumentType;
import com.documents4j.api.IConverter;
import com.documents4j.job.LocalConverter;
import jandcode.commons.UtFile;
import jandcode.commons.variant.IVariantMap;
import jandcode.core.web.HttpError;
import jandcode.core.web.action.BaseAction;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import tofi.api.mdl.ApiMeta;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.concurrent.TimeUnit;

import static com.documents4j.api.DocumentType.*;

public class LoadpdfAction extends BaseAction {

    ApinatorApi apiMeta() {
        return getApp().bean(ApinatorService.class).getApi("meta");
    }

    protected void onExec() throws Exception {
        IVariantMap params = getReq().getParams();

        DbFileStorageService dfsrv = apiMeta().get(ApiMeta.class).getDbFileStorageService();
        dfsrv.setModelName(params.getString("model"));
        DbFileStorageItem fi = dfsrv.getFile(params.getLong("id"));
        File fs;
        String fn;
        String fnn;
        try {
             fs = fi.getFile();
             fn = fi.getOriginalFilename();
             fnn = fn.replace(UtFile.ext(fn), "pdf");
             if ( !fn.toLowerCase().endsWith(".pdf") ) {
                 String tempDir = System.getProperty("java.io.tmpdir");
                 //fnn = fn.replace(UtFile.ext(fn), "pdf");
                 String destPath = tempDir + fnn;
                 String srcPath = fs.getAbsolutePath();
                 if (fn.toLowerCase().endsWith(".docx") || fn.toLowerCase().endsWith(".doc") ||
                         fn.toLowerCase().endsWith(".xlsx") || fn.toLowerCase().endsWith(".xls") ||
                            fn.toLowerCase().endsWith(".txt") || fn.toLowerCase().endsWith(".log")) {
                     DocumentType dt = getDocumentType(fn);
                     cnv2pdf(srcPath, destPath, dt);
                 } else if (fn.toLowerCase().endsWith(".jpg") || fn.toLowerCase().endsWith(".jpeg") ||
                         fn.toLowerCase().endsWith(".png") || fn.toLowerCase().endsWith(".bmp")) {
                     img2pdf(srcPath, destPath);
                 }
                 fs = new File(destPath);
             }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new HttpError(404);
        }

        var res = new DownFile(fs, fnn);

        getReq().render(res);
    }

    private static DocumentType getDocumentType(String fn) {
        DocumentType dt = RTF;
        if (fn.toLowerCase().endsWith(".docx") || fn.toLowerCase().endsWith(".doc")) {
            dt = MS_WORD;
        }
        if (fn.toLowerCase().endsWith(".xlsx") || fn.toLowerCase().endsWith(".xls")) {
            dt = MS_EXCEL;
        }
        if (fn.toLowerCase().endsWith(".txt") || fn.toLowerCase().endsWith(".log")) {
            dt = TEXT;
        }
        return dt;
    }

    protected void cnv2pdf(String src, String dst, DocumentType dt) throws Exception {
        try (InputStream docxInputStream = new FileInputStream(src);
             OutputStream pdfOutputStream = new FileOutputStream(dst)) {
            IConverter converter = LocalConverter.builder()
                    .workerPool(20, 25, 2, TimeUnit.SECONDS)
                    .processTimeout(5, TimeUnit.SECONDS)
                    .build();

            converter.convert(docxInputStream).as(dt)
                    .to(pdfOutputStream).as(PDF)
                    .execute();

            converter.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void img2pdf(String src, String dst) throws Exception {
        PDDocument document = new PDDocument();
        InputStream in = new FileInputStream(src);
        BufferedImage bimg = ImageIO.read(in);
        float width = bimg.getWidth();
        float height = bimg.getHeight();
        PDPage page = new PDPage(new PDRectangle(width, height));
        document.addPage(page);
        PDImageXObject img = LosslessFactory.createFromImage(document, bimg);    //new PDImage(document, new FileInputStream(imagePath));
        PDPageContentStream contentStream = new PDPageContentStream(document, page);
        contentStream.drawImage(img, 0, 0);
        contentStream.close();
        in.close();
        document.save(dst);
        document.close();
    }

}

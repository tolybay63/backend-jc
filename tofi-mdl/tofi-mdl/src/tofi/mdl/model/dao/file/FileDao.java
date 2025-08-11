package tofi.mdl.model.dao.file;

import jandcode.core.dbm.dao.BaseModelDao;
import tofi.mdl.model.utils.FileWrapper;

import java.io.File;

public class FileDao extends BaseModelDao {

    public FileWrapper download(String path) {
        File file = new File(path);
        FileWrapper wrapper = new FileWrapper();
        wrapper.setFile(file);
        return wrapper;
    }
    //


}

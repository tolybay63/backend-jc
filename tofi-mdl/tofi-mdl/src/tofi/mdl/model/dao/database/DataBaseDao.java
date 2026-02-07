package tofi.mdl.model.dao.database;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class DataBaseDao extends BaseModelDao {

    public String getIdMetaModel() throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.getIdMetaModel();
    }

    public Store load(Map<String, Object> params) throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.load(params);
    }

    public StoreRecord newRec() throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.newRec();
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        utils.delete(rec);
    }

    public Store loadDbForSelect(String lang) throws Exception {
        DataBaseMdbUtils utils = new DataBaseMdbUtils(getMdb(), "DataBase");
        return utils.loadDbForSelect(lang);
    }

}

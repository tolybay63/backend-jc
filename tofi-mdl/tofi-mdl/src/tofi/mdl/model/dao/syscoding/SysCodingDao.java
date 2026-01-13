package tofi.mdl.model.dao.syscoding;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.dao.stock.StockMdbUtils;

import java.util.Map;

public class SysCodingDao extends BaseModelDao {


    public Store load() throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        return utils.load();
    }

    public StoreRecord newRec(long stockGr) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SysCoding");
        return utils.newRec(stockGr);
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SysCoding");
        return utils.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SysCoding");
        return utils.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SysCoding");
        utils.delete(rec);
    }



}

package tofi.mdl.model.dao.stock;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class StockDao extends BaseModelDao {


    public Store loadStocks(long stockGr) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        return utils.loadStocks(stockGr);
    }

    public StoreRecord newRec(long stockGr) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        return utils.newRec(stockGr);
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        return utils.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        return utils.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        utils.delete(rec);
    }

    public Store loadStockForSelect() throws Exception {
        StockMdbUtils utils = new StockMdbUtils(getMdb(), "SourceStock");
        return utils.loadStockForSelect();
    }

}

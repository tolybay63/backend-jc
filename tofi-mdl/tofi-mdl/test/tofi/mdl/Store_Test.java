package tofi.mdl;

import jandcode.core.dbm.test.Dbm_Test;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class Store_Test extends Dbm_Test {

    /**
     * Заполнение store большим количеством записей
     */
    @Test
    @Disabled
    public void very_many_data() throws Exception {
        int countCols = 10;
        int countRows = 3_000_000;

        Store st = getMdb().createStore();
        st.addField("id", "long");
        for (int i = 1; i <= countCols; i++) {
            st.addField("f" + i, "string");
        }

        memory.save();
        stopwatch.start();
        //
        for (int i = 1; i <= countRows; i++) {
            StoreRecord r = st.add();
            r.set("id", i);

            for (int j = 1; j <= countCols; j++) {
                r.set("f" + j, "string-value-" + i + "-" + j);
            }
        }
        //
        stopwatch.stop();
        memory.printCur();
        memory.printDiff();

        utils.outTable(st, 10);

        //
        stopwatch.start("scan");
        for (StoreRecord rec : st) {
            long id = rec.getLong("id");
            String f1 = rec.getString("f1");
            String f10 = rec.getString("f10");
        }
        stopwatch.stop("scan");

    }


}

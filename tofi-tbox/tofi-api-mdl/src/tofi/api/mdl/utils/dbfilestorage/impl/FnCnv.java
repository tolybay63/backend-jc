package tofi.api.mdl.utils.dbfilestorage.impl;

import jandcode.commons.UtString;

import java.util.ArrayList;
import java.util.List;

/**
 * Конвертор для имен файлов в хранилище.
 * Преобразование id в имя файла.
 */
public class FnCnv {
    // количество файлов и каталогов. base+base = общее количество элементов в каталоге
    protected long base = 100;

    public FnCnv() {
    }

    public FnCnv(long base) {
        this.base = base;
    }

    public long getBase() {
        return base;
    }

    public void setBase(long base) {
        this.base = base;
    }

    public List<String> splitId(long id) {
        id = id + base + 1; // что бы всегда был каталог!
        List<String> res = new ArrayList<String>();
        long v;
        while (id > base) {
            v = id % base;
            id = id / base;
            res.add(0, "" + v);
        }
        res.add(0, "" + id);
        return res;
    }

    public String toFileName(long id) {
        List<String> z = splitId(id);
        if (z.size() > 1) {
            z.remove(z.size() - 1);
        }
        return UtString.join(z, "/") + "/" + id + ".dat";
    }


}

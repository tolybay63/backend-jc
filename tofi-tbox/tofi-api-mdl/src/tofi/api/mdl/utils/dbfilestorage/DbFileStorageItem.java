package tofi.api.mdl.utils.dbfilestorage;

import java.io.File;

/**
 * Представление файла из хранилища
 */
public abstract class DbFileStorageItem {
    /**
     * id файла
     */
    public abstract long getId();

    /**
     * Физический файл в хранилище
     */
    public abstract File getFile();

    /**
     * Оригинальное имя файла, с которым файл был помещен в хранилище
     */
    public abstract String getOriginalFilename();

    public String getPath() {
        return getFile().getAbsolutePath();
    }
}

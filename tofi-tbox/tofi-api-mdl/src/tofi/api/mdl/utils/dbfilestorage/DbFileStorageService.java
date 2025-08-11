package tofi.api.mdl.utils.dbfilestorage;

import jandcode.core.dbm.BaseModelMember;

import java.io.File;

/**
 * Возвращает путь до хранилища файлов
 */
public abstract class DbFileStorageService extends BaseModelMember {
    /**
     * Возвращает путь до хранилища файлов
     */
    public abstract String getStoragePath();

    /**
     * Получить файл из хранилища по id
     */
    public abstract DbFileStorageItem getFile(long id) throws Exception;

    /**
     * Добавить файл в хранилище
     *
     * @param f                оригинал файла
     * @param originalFilename оригинальное имя файла
     * @return добавленный файл
     */
    public abstract DbFileStorageItem addFile(File f, String originalFilename) throws Exception;

    /**
     * Добавить файл в хранилище
     *
     * @param f оригинал файла
     * @return добавленный файл
     */
    public DbFileStorageItem addFile(File f) throws Exception {
        return addFile(f, null);
    }

    /**
     * Удалить файл из хранилища по id
     */
    public abstract void removeFile(long id) throws Exception;

    public abstract void setModelName(String model) throws Exception;



}

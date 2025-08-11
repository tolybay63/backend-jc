package tofi.api.mdl.utils.dbfilestorage.impl;

import jandcode.commons.UtFile;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.std.DataDirService;
import jandcode.core.store.StoreRecord;
import org.apache.commons.io.FileUtils;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService;
import tofi.api.mdl.utils.dbfilestorage.FsDao;

import java.io.File;

public class DbFileStorageServiceImpl extends DbFileStorageService {
    protected FnCnv fnCnv = new FnCnv(500);  // НЕ МЕНЯТЬ 500! сломается генерация уникальных
    protected String storagePath;
    protected String model;

    @Override
    public void setModelName(String model) throws Exception {
        this.model = model;
    }

    public String getStoragePath() {
        if (storagePath == null) {
            synchronized (this) {
                if (storagePath == null) {
                    String sp = getApp().bean(DataDirService.class).getPath("dbfilestorage") + File.separator + this.model;
                    //String dbid = getModel().bean(DbInfoService.class).getDbId();
                    //todo id экземпляра базы временно

                    String dbid = ""; //getModel().bean(DbDriver.class).getName();
                    String s = UtFile.join(sp, dbid);
                    if (!UtFile.exists(s)) {
                        UtFile.mkdirs(s);
                    }
                    storagePath = s;
                }
            }
        }
        return storagePath;
    }

    protected DbFileStorageItemImpl createItem(long id, String path, String originalFilename) {
        return new DbFileStorageItemImpl(id, path, originalFilename);
    }


    public DbFileStorageItem getFile(long id) throws Exception {
        FsDao dao = getModel().createMdb().createDao(FsDao.class);
        StoreRecord r = dao.loadRec(id);
        //
        String realpath = UtFile.join(getStoragePath(), r.getString("path"));
        //
        return createItem(r.getLong("id"), realpath, r.getString("originalFilename"));
    }

    public DbFileStorageItem addFile(File f, String originalFilename) throws Exception {
        if (UtString.empty(originalFilename)) {
            originalFilename = f.getName();
        }

        FsDao dao = getModel().createMdb().createDao(FsDao.class);
        long id = dao.getNextId();
        //
        String path = fnCnv.toFileName(id);
        String realpath = UtFile.join(getStoragePath(), path);
        if (UtFile.exists(realpath)) {
            throw new XError("File {0} already exists. Check storage and database!", realpath);
        }
        //
        DbFileStorageItemImpl it = createItem(id, realpath, originalFilename);
        // копируем во временный
        File fTmp = new File(realpath + ".tmp");
        FileUtils.copyFile(f, fTmp);
        // после копирования переименовываем
        fTmp.renameTo(new File(realpath));

        // записываем в базу
        StoreRecord rec = dao.createRecord();
        rec.set("id", id);
        rec.set("path", path); // относительный путь!
        rec.set("originalFilename", originalFilename);
        dao.ins(rec);
        //
        return it;
    }

    public void removeFile(long id) throws Exception {
        if (id == 0) {
            return;
        }
        FsDao dao = getModel().createMdb().createDao(FsDao.class);
        // берем файл
        DbFileStorageItem it = getFile(id);
        File f = it.getFile();

        // удаляем из базы
        dao.del(id);

        // удаляем файл
        f.delete();
    }

}

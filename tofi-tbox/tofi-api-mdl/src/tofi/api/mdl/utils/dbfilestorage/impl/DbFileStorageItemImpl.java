package tofi.api.mdl.utils.dbfilestorage.impl;

import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem;

import java.io.File;

public class DbFileStorageItemImpl extends DbFileStorageItem {

    private long id;
    private final String originalFilename;
    private final String path;

    public DbFileStorageItemImpl(long id, String path, String originalFilename) {
        this.id = id;
        this.path = path;
        this.originalFilename = originalFilename;
    }


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public File getFile() {
        return new File(path);
    }
}

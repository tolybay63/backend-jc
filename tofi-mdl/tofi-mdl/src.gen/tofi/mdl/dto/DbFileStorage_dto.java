/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Хранилище файлов
* <p>
    * table: DbFileStorage
    */
    public class DbFileStorage_dto {

    private Long id;

    @FieldProps(size = 255)
    private String path;

    @FieldProps(size = 255)
    private String originalFilename;

    /**
    * id
    */
    public Long getId() {
    return this.id;
    }

    public void setId(Long v) {
    this.id = v;
    }

    /**
    * Физический путь в хранилище
    */
    public String getPath() {
    return this.path;
    }

    public void setPath(String v) {
    this.path = v;
    }

    /**
    * Оригинальное имя файла
    */
    public String getOriginalFilename() {
    return this.originalFilename;
    }

    public void setOriginalFilename(String v) {
    this.originalFilename = v;
    }

    }

/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Базы данных
* <p>
    * table: DataBase
    */
    public class DataBase_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(size = 32)
    private String modelName;

    @FieldProps(dict = "FD_DataBaseType")
    private Long dataBaseType;

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
    * Код
    */
    public String getCod() {
    return this.cod;
    }

    public void setCod(String v) {
    this.cod = v;
    }

    /**
    * Идентификатор базы данных
    */
    public String getModelName() {
    return this.modelName;
    }

    public void setModelName(String v) {
    this.modelName = v;
    }

    /**
    * Тип базы данных
    */
    public Long getDataBaseType() {
    return this.dataBaseType;
    }

    public void setDataBaseType(Long v) {
    this.dataBaseType = v;
    }

    }

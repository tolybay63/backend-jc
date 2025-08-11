/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Сущности ТОФИ
* <p>
    * table: FD_EntityType
    */
    public class FDEntityType_dto {

    private Long id;

    @FieldProps(size = 200)
    private String text;

    private Boolean vis;

    private Long ord;

    @FieldProps(size = 20)
    private String code;

    @FieldProps(size = 30)
    private String tableName;

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
    * Наименование
    */
    public String getText() {
    return this.text;
    }

    public void setText(String v) {
    this.text = v;
    }

    /**
    * Видимость
    */
    public Boolean getVis() {
    return this.vis;
    }

    public void setVis(Boolean v) {
    this.vis = v;
    }

    /**
    * Порядок
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    /**
    * Код
    */
    public String getCode() {
    return this.code;
    }

    public void setCode(String v) {
    this.code = v;
    }

    /**
    * Имя таблицы
    */
    public String getTableName() {
    return this.tableName;
    }

    public void setTableName(String v) {
    this.tableName = v;
    }

    }

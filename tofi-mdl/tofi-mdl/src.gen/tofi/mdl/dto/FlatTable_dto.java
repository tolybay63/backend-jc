/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Плоские таблицы
* <p>
    * table: FlatTable
    */
    public class FlatTable_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(size = 50)
    private String nameTable;

    private Long relCls;

    private Long cls;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

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
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
    }

    /**
    * Название таблицы
    */
    public String getNameTable() {
    return this.nameTable;
    }

    public void setNameTable(String v) {
    this.nameTable = v;
    }

    /**
    * Классы отношений
    */
    public Long getRelCls() {
    return this.relCls;
    }

    public void setRelCls(Long v) {
    this.relCls = v;
    }

    /**
    * Классы
    */
    public Long getCls() {
    return this.cls;
    }

    public void setCls(Long v) {
    this.cls = v;
    }

    /**
    * Краткое наименование
    */
    public String getName() {
    return this.name;
    }

    public void setName(String v) {
    this.name = v;
    }

    /**
    * Полное наименование
    */
    public String getFullName() {
    return this.fullName;
    }

    public void setFullName(String v) {
    this.fullName = v;
    }

    /**
    * Комментарий
    */
    public String getCmt() {
    return this.cmt;
    }

    public void setCmt(String v) {
    this.cmt = v;
    }

    }

/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Измерения объектов и отношений
* <p>
    * table: DimObj
    */
    public class DimObj_dto {

    private Long id;

    private Long dimObjGr;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

    private Long ord;

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
    * Группы измерений объектов и отношений
    */
    public Long getDimObjGr() {
    return this.dimObjGr;
    }

    public void setDimObjGr(Long v) {
    this.dimObjGr = v;
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

    /**
    * Порядковый номер
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    }

/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Типы объектов
* <p>
    * table: Typ
    */
    public class Typ_dto {

    private Long id;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    private Long parent;

    @FieldProps(size = 30)
    private String cod;

    private Boolean isOpenness;

    @FieldProps(dict = "FD_TypCategory")
    private Long typCategory;

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
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
    }

    /**
    * Родительский тип
    */
    public Long getParent() {
    return this.parent;
    }

    public void setParent(Long v) {
    this.parent = v;
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
    * Открытость типа
    */
    public Boolean getIsOpenness() {
    return this.isOpenness;
    }

    public void setIsOpenness(Boolean v) {
    this.isOpenness = v;
    }

    /**
    * Категория типа объектов
    */
    public Long getTypCategory() {
    return this.typCategory;
    }

    public void setTypCategory(Long v) {
    this.typCategory = v;
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

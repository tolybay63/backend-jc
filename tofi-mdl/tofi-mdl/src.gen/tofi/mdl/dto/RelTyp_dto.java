/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Отношения между типами объектов
* <p>
    * table: RelTyp
    */
    public class RelTyp_dto {

    private Long id;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(size = 30)
    private String cod;

    private Boolean isOpenness;

    private Integer card;

    @FieldProps(size = 60)
    private String icon;

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
    * Число кардинальности
    */
    public Integer getCard() {
    return this.card;
    }

    public void setCard(Integer v) {
    this.card = v;
    }

    /**
    * Иконка
    */
    public String getIcon() {
    return this.icon;
    }

    public void setIcon(String v) {
    this.icon = v;
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

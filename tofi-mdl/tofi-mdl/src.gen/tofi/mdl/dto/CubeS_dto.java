/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Стандартные кубы
* <p>
    * table: CubeS
    */
    public class CubeS_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    private Long cubeSGr;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(dict = "FD_CubeSType")
    private Long cubeSType;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private XDate dOrg;

    private Long ord;

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
    * Группы стандартных кубов
    */
    public Long getCubeSGr() {
    return this.cubeSGr;
    }

    public void setCubeSGr(Long v) {
    this.cubeSGr = v;
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
    * Тип куба
    */
    public Long getCubeSType() {
    return this.cubeSType;
    }

    public void setCubeSType(Long v) {
    this.cubeSType = v;
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
    * dOrg
    */
    public XDate getDOrg() {
    return this.dOrg;
    }

    public void setDOrg(XDate v) {
    this.dOrg = v;
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

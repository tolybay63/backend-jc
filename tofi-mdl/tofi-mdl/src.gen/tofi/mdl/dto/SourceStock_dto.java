/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Источники и стоки данных
* <p>
    * table: SourceStock
    */
    public class SourceStock_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    private Long parent;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    @FieldProps(dict = "FD_SourceStockType")
    private Long sourceStockType;

    private Boolean isOwn;

    private XDate dbeg;

    private XDate dend;

    @FieldProps(size = 30)
    private String host;

    @FieldProps(size = 30)
    private String portNumber;

    @FieldProps(size = 30)
    private String usrLogin;

    @FieldProps(size = 64)
    private String usrPassword;

    @FieldProps(size = 64)
    private Long formatType;

    @FieldProps(size = 100)
    private String sourceStockPath;

    @FieldProps(size = 30)
    private String fileNameRegExp;

    @FieldProps(size = 30)
    private String uRL;

    @FieldProps(size = 30)
    private String driver;

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
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
    }

    /**
    * Источники и стоки данных
    */
    public Long getParent() {
    return this.parent;
    }

    public void setParent(Long v) {
    this.parent = v;
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
    * Тип источника данных
    */
    public Long getSourceStockType() {
    return this.sourceStockType;
    }

    public void setSourceStockType(Long v) {
    this.sourceStockType = v;
    }

    /**
    * Собсвенный источник или сток данных
    */
    public Boolean getIsOwn() {
    return this.isOwn;
    }

    public void setIsOwn(Boolean v) {
    this.isOwn = v;
    }

    /**
    * Начало интервала
    */
    public XDate getDbeg() {
    return this.dbeg;
    }

    public void setDbeg(XDate v) {
    this.dbeg = v;
    }

    /**
    * Конец интервала
    */
    public XDate getDend() {
    return this.dend;
    }

    public void setDend(XDate v) {
    this.dend = v;
    }

    /**
    * Хост
    */
    public String getHost() {
    return this.host;
    }

    public void setHost(String v) {
    this.host = v;
    }

    /**
    * Номер порта
    */
    public String getPortNumber() {
    return this.portNumber;
    }

    public void setPortNumber(String v) {
    this.portNumber = v;
    }

    /**
    * Логин
    */
    public String getUsrLogin() {
    return this.usrLogin;
    }

    public void setUsrLogin(String v) {
    this.usrLogin = v;
    }

    /**
    * Пароль
    */
    public String getUsrPassword() {
    return this.usrPassword;
    }

    public void setUsrPassword(String v) {
    this.usrPassword = v;
    }

    /**
    * Формат данных
    */
    public Long getFormatType() {
    return this.formatType;
    }

    public void setFormatType(Long v) {
    this.formatType = v;
    }

    /**
    * Путь к папке
    */
    public String getSourceStockPath() {
    return this.sourceStockPath;
    }

    public void setSourceStockPath(String v) {
    this.sourceStockPath = v;
    }

    /**
    * Шаблон для имени файлов
    */
    public String getFileNameRegExp() {
    return this.fileNameRegExp;
    }

    public void setFileNameRegExp(String v) {
    this.fileNameRegExp = v;
    }

    /**
    * URL-адрес
    */
    public String getURL() {
    return this.uRL;
    }

    public void setURL(String v) {
    this.uRL = v;
    }

    /**
    * Драйвер БД
    */
    public String getDriver() {
    return this.driver;
    }

    public void setDriver(String v) {
    this.driver = v;
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

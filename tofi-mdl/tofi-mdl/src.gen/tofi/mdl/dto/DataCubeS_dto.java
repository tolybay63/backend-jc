/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Данные по формированию стандартных кубов
* <p>
    * table: DataCubeS
    */
    public class DataCubeS_dto {

    private Long id;

    private Long cubeS;

    @FieldProps(dict = "FD_CubeSActionType")
    private Long cubeSActionType;

    private XDateTime dtBeg;

    private XDateTime dtEnd;

    private Long authUser;

    private Long countProCubeOwn;

    private Long countProCubeProp;

    private Long countProCubePeriod;

    private Long countCells;

    private Long countDataCells;

    private Boolean isComplete;

    private Boolean isSheduled;

    @FieldProps(size = 2000)
    private String errorMessage;

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
    * cubeS
    */
    public Long getCubeS() {
    return this.cubeS;
    }

    public void setCubeS(Long v) {
    this.cubeS = v;
    }

    /**
    * Тип действий с кубами
    */
    public Long getCubeSActionType() {
    return this.cubeSActionType;
    }

    public void setCubeSActionType(Long v) {
    this.cubeSActionType = v;
    }

    /**
    * Дата и время начала действия
    */
    public XDateTime getDtBeg() {
    return this.dtBeg;
    }

    public void setDtBeg(XDateTime v) {
    this.dtBeg = v;
    }

    /**
    * Дата и время окончания действия
    */
    public XDateTime getDtEnd() {
    return this.dtEnd;
    }

    public void setDtEnd(XDateTime v) {
    this.dtEnd = v;
    }

    /**
    * authUser
    */
    public Long getAuthUser() {
    return this.authUser;
    }

    public void setAuthUser(Long v) {
    this.authUser = v;
    }

    /**
    * Количество ячеек прокуба объектов
    */
    public Long getCountProCubeOwn() {
    return this.countProCubeOwn;
    }

    public void setCountProCubeOwn(Long v) {
    this.countProCubeOwn = v;
    }

    /**
    * Количество ячеек прокуба свойств
    */
    public Long getCountProCubeProp() {
    return this.countProCubeProp;
    }

    public void setCountProCubeProp(Long v) {
    this.countProCubeProp = v;
    }

    /**
    * Количество ячеек измерения периодов
    */
    public Long getCountProCubePeriod() {
    return this.countProCubePeriod;
    }

    public void setCountProCubePeriod(Long v) {
    this.countProCubePeriod = v;
    }

    /**
    * Количество ячеек куба
    */
    public Long getCountCells() {
    return this.countCells;
    }

    public void setCountCells(Long v) {
    this.countCells = v;
    }

    /**
    * Количество заполненных ячеек куба
    */
    public Long getCountDataCells() {
    return this.countDataCells;
    }

    public void setCountDataCells(Long v) {
    this.countDataCells = v;
    }

    /**
    * Статус завершения
    */
    public Boolean getIsComplete() {
    return this.isComplete;
    }

    public void setIsComplete(Boolean v) {
    this.isComplete = v;
    }

    /**
    * Статус действия
    */
    public Boolean getIsSheduled() {
    return this.isSheduled;
    }

    public void setIsSheduled(Boolean v) {
    this.isSheduled = v;
    }

    /**
    * Сообщение
    */
    public String getErrorMessage() {
    return this.errorMessage;
    }

    public void setErrorMessage(String v) {
    this.errorMessage = v;
    }

    }

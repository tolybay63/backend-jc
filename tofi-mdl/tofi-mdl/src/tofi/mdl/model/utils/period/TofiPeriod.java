package tofi.mdl.model.utils.period;

import jandcode.commons.datetime.XDate;
import tofi.mdl.consts.FD_PeriodType_consts;
import tofi.mdl.model.utils.AppConst;

public class TofiPeriod {
    protected String name;
    protected XDate dbeg;
    protected XDate dend;
    protected long periodType;

    public TofiPeriod() {
        name = "";
        dbeg = AppConst.DBEG_EMPTY;
        dend = AppConst.DEND_EMPTY;
        periodType = FD_PeriodType_consts.day;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setDbeg(XDate dbeg) {
        this.dbeg = dbeg;
    }

    public XDate getDbeg() {
        return dbeg;
    }

    public void setDend(XDate dend) {
        this.dend = dend;
    }

    public XDate getDend() {
        return dend;
    }

    public void setPeriodType(long periodType) {
        this.periodType = periodType;
    }

    public long getPeriodType() {
        return periodType;
    }

}

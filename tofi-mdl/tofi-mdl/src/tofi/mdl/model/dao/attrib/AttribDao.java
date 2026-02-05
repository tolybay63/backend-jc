package tofi.mdl.model.dao.attrib;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class AttribDao extends BaseModelDao {

    public Store loadAttrib(Map<String, Object> params)  throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        return u.loadAttrib(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        return u.insert(params);
    }

    public Store loadAttribChar(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        return u.loadAttribChar(params);
    }

    public Store insertChar(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "AttribChar");
        return u.insertAttribChar(params);
    }

    public Store updateChar(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "AttribChar");
        return u.updateAttribChar(params);
    }

    public void deleteChar(Map<String, Object> params) throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "AttribChar");
        u.deleteAttribChar(params);
    }

    public Store loadForSelect() throws Exception {
        AttribMdbUtils u = new AttribMdbUtils(getMdb(), "Attrib");
        return u.loadForSelect();
    }

}

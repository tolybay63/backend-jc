package dtj.inspection.dao

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory

class ImportDao extends BaseMdbUtils {
    ApinatorApi apiMeta() { return app.bean(ApinatorService).getApi("meta") }

    @DaoMethod
    void analyze(File file) {
        try {
            //File inputFile = new File("C:\\jc-2\\_info\\xml\\G057_22042025_113706_64_1 1.xml")
            //File inputFile = new File("C:\\jc-2\\_info\\xml\\B057_22042025_113706_1 1.xml")
            if (file.name.startsWith("G")) {
                parseOtstup(file)
                assignPropDefault("_otstup")
            } else if (file.name.startsWith("B")) {
                parseBall(file)
                assignPropDefault("_ball")
                check("_ball")
            }
        } catch (Exception e) {
            e.printStackTrace()
        }
    }

    void check(String domain) {
        //1 План работы (plandata)
        //Store stPlan =


        //2 Осмотр и проверок


        //3 Журнал параметров


    }

    void assignPropDefault(String domain) {
        Map<String, String> mapCoding = new HashMap<>()
        if (domain == "_ball") {
            //
            mapCoding.put("date_obn", "Prop_FactDateEnd")
            mapCoding.put("nomer_mdk", "Prop_NumberTrack")
            mapCoding.put("avtor", "Prop_HeadTrack")
            mapCoding.put("km", "Prop_StartKm")
            mapCoding.put("pk", "Prop_StartPicket")
            mapCoding.put("ballkm", "Prop_ParamsLimit")
            mapCoding.put("kol_ots", "Prop_NumberRetreat")
            //
            long idSysCoding = 1000
            Store stTmp = apiMeta().get(ApiMeta).loadSql("""
                select sc.id, sc.cod, scc.syscod, scc.cod as codOther
                from syscoding sc
                    left join syscodingcod scc on scc.syscoding=sc.id
                where sc.id=${idSysCoding} and scc.cod in (${"'" + mapCoding.keySet().join("','") + "'"})
            """, "")
            Set<Object> codsOther = stTmp.getUniqueValues("codOther")
            Store stSysCod = apiMeta().get(ApiMeta).loadSql("""
                select id, cod from SysCod where cod in (${"'" + mapCoding.values().join("','") + "'"})
            """, "")
            StoreIndex indSysCod = stSysCod.getIndex("cod")
            for (Map.Entry entry : mapCoding) {
                if (!codsOther.contains(entry.key)) {
                    StoreRecord rec = mdb.createStoreRecord("SysCodingCod")
                    StoreRecord recInd = indSysCod.get(entry.value.toString())
                    if (recInd != null)
                        rec.set("syscod", recInd.getLong("id"))
                    rec.set("sysCoding", idSysCoding)
                    rec.set("cod", entry.key)
                    apiMeta().get(ApiMeta).insertRecToTable("SysCodingCod", rec.getValues(), true)
                }
            }
        }
    }


    void parseOtstup(File inputFile) {
        String filename = inputFile.name

        System.out.println("Импорт файла: ${filename}")

        try {
            mdb.startTran()
            mdb.execQuery("delete from _otstup")
            //
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = rowElement.getElementsByTagName("REC").item(0).getTextContent()
                    String kod_otstup = rowElement.getElementsByTagName("kod_otstup").item(0).getTextContent()

                    String prizn_most_s = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    Long prizn_most = null
                    if (!prizn_most_s.isEmpty())
                        prizn_most = UtCnv.toLong(prizn_most)
                    //
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    String time_obn = rowElement.getElementsByTagName("time_obn").item(0).getTextContent()
                    String y = date_obn.substring(6)
                    String m = date_obn.substring(3, 5)
                    String d = date_obn.substring(0, 2)
                    String hh = time_obn.substring(0, 2)
                    String mm = time_obn.substring(3, 5)
                    String ss = time_obn.substring(6)
                    String dte = XDateTime.create(UtCnv.toInt(y), UtCnv.toInt(m), UtCnv.toInt(d),
                            UtCnv.toInt(hh), UtCnv.toInt(mm), UtCnv.toInt(ss))
                    //
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    //
                    String km_s = UtCnv.toLong(rowElement.getElementsByTagName("km").item(0).getTextContent())
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = UtCnv.toLong(rowElement.getElementsByTagName("pk").item(0).getTextContent())
                    Long pk = null
                    if (!pk_s.isEmpty())
                        pk = UtCnv.toLong(pk_s)

                    String metr_s = rowElement.getElementsByTagName("metr").item(0).getTextContent()
                    Long metr = null
                    if (!metr_s.isEmpty())
                        metr = UtCnv.toLong(metr_s)

                    String dlina_ots_s = rowElement.getElementsByTagName("dlina_ots").item(0).getTextContent()
                    Long dlina_ots = null
                    if (!dlina_ots_s.isEmpty())
                        dlina_ots = UtCnv.toLong(dlina_ots_s)

                    String velich_ots_s = rowElement.getElementsByTagName("velich_ots").item(0).getTextContent()
                    Long velich_ots = null
                    if (!velich_ots_s.isEmpty())
                        velich_ots = UtCnv.toLong(velich_ots_s)

                    String glub_ots_s = rowElement.getElementsByTagName("glub_ots").item(0).getTextContent()
                    Long glub_ots = null
                    if (!glub_ots_s.isEmpty())
                        glub_ots = UtCnv.toLong(glub_ots_s)

                    String stepen_ots_s = rowElement.getElementsByTagName("stepen_ots").item(0).getTextContent()
                    Long stepen_ots = null
                    if (!stepen_ots_s.isEmpty())
                        stepen_ots = UtCnv.toLong(stepen_ots_s)

                    String kol_ots_s = UtCnv.toLong(rowElement.getElementsByTagName("kol_ots").item(0).getTextContent())
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _otstup (rec,kod_otstup,prizn_most,datetime_obn,nomer_mdk,avtor,km,pk,metr,dlina_ots,velich_ots,glub_ots,stepen_ots,kol_ots)
                        VALUES ($ind,$kod_otstup,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$metr, $dlina_ots,$velich_ots,$glub_ots,$stepen_ots,$kol_ots);
                    """)
                }
            }
            mdb.execQueryNative("""
                INSERT INTO public._log (filename, datetime_create, filled, datetime_fill)
                VALUES('${filename}', '${XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)}', 0, null);
            """)
        } catch (Exception e) {
            e.printStackTrace()
            mdb.rollback()
        } finally {
            mdb.commit()
            Store st = mdb.loadQuery("select * from _otstup where 0=0")
            mdb.outTable(st)
        }
    }

    void parseBall(File inputFile) {
        String filename = inputFile.name

        System.out.println("Импорт файла: ${filename}")

        try {
            mdb.startTran()
            mdb.execQuery("delete from _ball")
            //
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()
            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = UtCnv.toLong(rowElement.getElementsByTagName("REC").item(0).getTextContent())
                    String kod_napr_s = UtCnv.toLong(rowElement.getElementsByTagName("kod_napr").item(0).getTextContent())
                    Long kod_napr = null
                    if (!kod_napr_s.isEmpty())
                        kod_napr = UtCnv.toLong(kod_napr_s)


                    String prizn_most_s = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    Long prizn_most = null
                    if (!prizn_most_s.isEmpty())
                        prizn_most = UtCnv.toLong(prizn_most)
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    //
                    String y = date_obn.substring(6)
                    String m = date_obn.substring(3, 5)
                    String d = date_obn.substring(0, 2)
                    String dte = XDate.create(UtCnv.toInt(y), UtCnv.toInt(m), UtCnv.toInt(d))
                    //
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    String km_s = UtCnv.toLong(rowElement.getElementsByTagName("km").item(0).getTextContent())
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = UtCnv.toLong(rowElement.getElementsByTagName("pk").item(0).getTextContent())
                    Long pk = null
                    if (!pk_s.isEmpty())
                        pk = UtCnv.toLong(pk_s)

                    String ballkm_s = UtCnv.toLong(rowElement.getElementsByTagName("ballkm").item(0).getTextContent())
                    Long ballkm = null
                    if (!ballkm_s.isEmpty())
                        ballkm = UtCnv.toLong(ballkm_s)

                    String kol_ots_s = UtCnv.toLong(rowElement.getElementsByTagName("kol_ots").item(0).getTextContent())
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _ball (rec,kod_napr,prizn_most,date_obn,nomer_mdk,avtor,km,pk,ballkm,kol_ots)
                        VALUES ($ind,$kod_napr,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$ballkm,$kol_ots);
                    """)
                }
            }
            mdb.execQueryNative("""
                INSERT INTO public._log (filename, datetime_create, filled, datetime_fill)
                VALUES('${filename}', '${XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)}', 0, null);
            """)
        } catch (Exception e) {
            e.printStackTrace()
            mdb.rollback()
        } finally {
            mdb.commit()
            Store st = mdb.loadQuery("select * from _ball where 0=0")
            mdb.outTable(st)
        }

    }


}

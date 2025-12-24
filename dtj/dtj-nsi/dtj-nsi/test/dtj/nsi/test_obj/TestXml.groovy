package dtj.nsi.test_obj


import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory

class TestXml extends Apx_Test {

    @Test
    void test() {
        try {
            //File inputFile = new File("D:\\backup\\xml\\G057_22042025_113706_64_1 1.xml")
            File inputFile = new File("D:\\backup\\xml\\B057_22042025_113706_1 1.xml")
            if (inputFile.name.startsWith("G"))
                parseOtstup(inputFile)
            else if (inputFile.name.startsWith("B"))
                parseBall(inputFile)
        } catch (Exception e) {
            e.printStackTrace()
        }

    }


    void parseOtstup(File inputFile) {
        try {
            mdb.startTran()
            mdb.execQuery("delete from _otstup")
        } finally {
            mdb.commit()
        }

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            mdb.startTran()
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = rowElement.getElementsByTagName("REC").item(0).getTextContent()
                    String kod_otstup = rowElement.getElementsByTagName("kod_otstup").item(0).getTextContent()
                    String prizn_most = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
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
                    String km = rowElement.getElementsByTagName("km").item(0).getTextContent()
                    String pk = rowElement.getElementsByTagName("pk").item(0).getTextContent()
                    String metr = rowElement.getElementsByTagName("metr").item(0).getTextContent()
                    String dlina_ots = rowElement.getElementsByTagName("dlina_ots").item(0).getTextContent()
                    String velich_ots = rowElement.getElementsByTagName("velich_ots").item(0).getTextContent()
                    String glub_ots = rowElement.getElementsByTagName("glub_ots").item(0).getTextContent()
                    String stepen_ots = rowElement.getElementsByTagName("stepen_ots").item(0).getTextContent()
                    String kol_ots = rowElement.getElementsByTagName("kol_ots").item(0).getTextContent()
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _otstup (rec,kod_otstup,prizn_most,datetime_obn,nomer_mdk,avtor,km,pk,metr,dlina_ots,velich_ots,glub_ots,stepen_ots,kol_ots)
                        VALUES ($ind,$kod_otstup,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$metr, $dlina_ots,$velich_ots,$glub_ots,$stepen_ots,$kol_ots);
                    """)
                }
            }
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            mdb.commit()
            Store st = mdb.loadQuery("select * from _otstup where 0=0")
            mdb.outTable(st)
        }
    }

    void parseBall(File inputFile) {
        try {
            mdb.startTran()
            mdb.execQuery("delete from _ball")
        } finally {
            mdb.commit()
        }

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            mdb.startTran()
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = UtCnv.toLong(rowElement.getElementsByTagName("REC").item(0).getTextContent())
                    String prizn_most = UtCnv.toLong(rowElement.getElementsByTagName("prizn_most").item(0).getTextContent())
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    //
                    String y = date_obn.substring(6)
                    String m = date_obn.substring(3, 5)
                    String d = date_obn.substring(0, 2)
                    String dte = XDate.create(UtCnv.toInt(y), UtCnv.toInt(m), UtCnv.toInt(d))
                    //
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    String km = UtCnv.toLong(rowElement.getElementsByTagName("km").item(0).getTextContent())
                    String pk = UtCnv.toLong(rowElement.getElementsByTagName("pk").item(0).getTextContent())
                    String ballkm = UtCnv.toLong(rowElement.getElementsByTagName("ballkm").item(0).getTextContent())
                    String kol_ots = UtCnv.toLong(rowElement.getElementsByTagName("kol_ots").item(0).getTextContent())
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _ball (rec,prizn_most,date_obn,nomer_mdk,avtor,km,pk,ballkm,kol_ots)
                        VALUES ($ind,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$ballkm,$kol_ots);
                    """)
                }
            }
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            mdb.commit()
            Store st = mdb.loadQuery("select * from _ball where 0=0")
            mdb.outTable(st)
        }

    }

}

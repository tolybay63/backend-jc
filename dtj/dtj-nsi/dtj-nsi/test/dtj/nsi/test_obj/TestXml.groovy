package dtj.nsi.test_obj

import io.undertow.server.handlers.ExceptionHandler
import jandcode.core.apx.test.Apx_Test
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


    static void parseOtstup(File inputFile) {
        Set<String> setKodOtstup = new HashSet<>()
        Set<String> setKodNapr = new HashSet<>()

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            System.out.println("rec\tkod_otstup\tkod_napr\tprizn_most\tdate_obn\ttime_obn\tnomer_mdk\tavtor\tkm\tpk\tmetr\tdlina_ots\tvelich_ots\tglub_ots\tstepen_ots\tkol_ots")
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String rec = rowElement.getElementsByTagName("REC").item(0).getTextContent()
                    String kod_otstup = rowElement.getElementsByTagName("kod_otstup").item(0).getTextContent()
                    if (kod_otstup)
                        setKodOtstup.add("kod_otstup_"+kod_otstup)
                    String kod_napr = rowElement.getElementsByTagName("kod_napr").item(0).getTextContent()
                    if (kod_napr)
                        setKodNapr.add("kod_napr_"+kod_napr)
                    String prizn_most = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    String time_obn = rowElement.getElementsByTagName("time_obn").item(0).getTextContent()
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

                    System.out.println("${rec}\t${kod_otstup}\t${kod_napr}\t${prizn_most}\t${date_obn}\t${time_obn}\t${nomer_mdk}\t${avtor}\t${km}\t${pk}\t${metr}\t${dlina_ots}\t${velich_ots}\t${glub_ots}\t${stepen_ots}\t${kol_ots}")
                }
            }
        } catch (Exception e) {
            e.printStackTrace()
        }
        System.out.println(setKodOtstup)
        System.out.println(setKodNapr)
    }

    static void parseBall(File inputFile) {
        Set<String> setKodOtstup = new HashSet<>()
        Set<String> setKodNapr = new HashSet<>()
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            System.out.println("rec\tkod_napr\tprizn_most\tdate_obn\tnomer_mdk\tavtor\tkm\tpk\tballkm\tkol_ots")
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String rec = rowElement.getElementsByTagName("REC").item(0).getTextContent()
                    String kod_napr = rowElement.getElementsByTagName("kod_napr").item(0).getTextContent()
                    if (kod_napr)
                        setKodNapr.add("kod_napr_"+kod_napr)
                    String prizn_most = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    String km = rowElement.getElementsByTagName("km").item(0).getTextContent()
                    String pk = rowElement.getElementsByTagName("pk").item(0).getTextContent()
                    String ballkm = rowElement.getElementsByTagName("ballkm").item(0).getTextContent()
                    String kol_ots = rowElement.getElementsByTagName("kol_ots").item(0).getTextContent()

                    System.out.println("${rec}\t${kod_napr}\t${prizn_most}\t${date_obn}\t${nomer_mdk}\t${avtor}\t${km}\t${pk}\t${ballkm}\t${kol_ots}")
                }
            }
        } catch (Exception e) {
            e.printStackTrace()
        }

        System.out.println(setKodNapr)

    }

}

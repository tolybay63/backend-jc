package dtj.nsi.dao.utils

import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.dbm.domain.Domain
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook

class XLSXReader_withoutDescription {

    protected Mdb mdb
    protected File file
    private Domain domain
    private Store store
    private int countRows
    private List<String> fields = new ArrayList<>()

    int getCountRows() {
        return countRows
    }

    List<String> getFields() {
        return fields
    }

    Domain getDomain() {
        return domain
    }

    void setDomain(Domain domain) {
        this.domain = domain
    }

    Store getStore() {
        return store
    }

    void setStore(Store store) {
        this.store = store
    }

    void setFile(File f) {
        file = f
    }

    File getFile() {
        if (file == null) {
            throw new XError("Не указан файл")
        }
        return file
    }

    XLSXReader_withoutDescription(Mdb mdb, File f, Domain d, Store s){
        this.mdb = mdb
        setFile(f)
        setDomain(d)
        setStore(s)
        createDomain()


    }

    void eachRow(Object eachFN){
        InputStream xls_file = new FileInputStream( getFile() )
        try {
            XSSFWorkbook workbook = new XSSFWorkbook( xls_file )
            XSSFSheet sheet = workbook.getSheetAt(0)
            Iterator < Row > rowIterator = sheet.iterator()
            Row row = rowIterator.next()
            Iterator < Cell > cellIterator = row.iterator()
            Map fields = [:]
            while ( cellIterator.hasNext() ) {
                Cell cell = cellIterator.next()
                String field_name =  cell.getStringCellValue()
                String key_ci = cell.columnIndex.toString()
                fields.put( key_ci, field_name )
            }
            while ( rowIterator.hasNext() ) {
                row = rowIterator.next()
                cellIterator = row.iterator()
                Map map = [:]
                while ( cellIterator.hasNext() ) {
                    Cell cell = cellIterator.next()
                    String key_ci = cell.columnIndex.toString()
                    switch ( cell.getCellType() ) {

                        case CellType.BOOLEAN : map.put( fields.get(key_ci), cell.getBooleanCellValue() )
                            break
                        case CellType.NUMERIC :
                            double v = cell.getNumericCellValue()
                            long l = UtCnv.toLong(cell.getNumericCellValue())
                            double r = v - l
                            if (r != 0)
                                map.put( fields.get(key_ci), UtCnv.toDouble(cell.getNumericCellValue()) )
                            else
                                map.put( fields.get(key_ci), UtCnv.toLong(cell.getNumericCellValue()) )
                            break

                        case CellType.STRING : map.put( fields.get(key_ci), cell.getStringCellValue() )
                            break
                    }
                }
                if ( eachFN != null ) {
                    eachFN( map )
                }
            }
        } finally {
            xls_file.close()
        }
    }

    private void createDomain() {
        InputStream xls_file = new FileInputStream( getFile() )
        try {

            XSSFWorkbook workbook = new XSSFWorkbook( xls_file )
            XSSFSheet sheet = workbook.getSheetAt(0)
            countRows = sheet.getLastRowNum()
            Row row = sheet.getRow(0)
            Iterator < Cell > cellIterator = row.iterator()
            while ( cellIterator.hasNext() ) {
                Cell cell = cellIterator.next()
                if ( cell.getCellType() == CellType.STRING ) {
                    store.addField(cell.getStringCellValue(), "string")
                    fields.add(cell.getStringCellValue())
                }
                else
                    throw new XError( "Error in header" )
            }
        } finally {
            xls_file.close()
        }
    }

}

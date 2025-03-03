class OracleTypeHandler:
    def handle_lob(self, lob):
        if isinstance(lob, cx_Oracle.LOB):
            return lob.read()
        return lob
        
    def handle_interval(self, interval):
        return str(interval)
        
    def handle_rowid(self, rowid):
        return str(rowid)
        
    def handle_xml(self, xml):
        return xml.getBytes().decode()
        
    def handle_geometry(self, sdo_geometry):
        return json.dumps({
            'type': sdo_geometry.geomType.name,
            'coordinates': sdo_geometry.getPoint()
        })
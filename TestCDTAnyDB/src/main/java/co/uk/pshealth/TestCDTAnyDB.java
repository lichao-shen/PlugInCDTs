package co.uk.pshealth;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.appian.types.AnyCDTs;
import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.MessageContainer;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.framework.SmartServiceContext;

import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement; 

@PaletteInfo(paletteCategory = "Custom Services", palette = "Database Services") 
public class TestCDTAnyDB extends AppianSmartService {

	// Query to get user-defined table type column name and type.
	private static final String USER_TABLE_TYPE_QUERY = "SELECT COL.name AS name, ST.name AS type FROM sys.table_types TYPE JOIN sys.columns"
			+ " COL ON TYPE.type_table_object_id = COL.object_id" + " JOIN sys.systypes AS ST ON ST.xtype = COL.system_type_id where TYPE.is_user_defined = 1 and TYPE.name = ?";

    private static String connectionUrl = "jdbc:sqlserver://172.25.16.52:1433;databaseName=engineering_secondary;user=pshealth.admin;password=appian@123";  
    
	private static final HashMap<String,Integer> TYPE_MAPPING;	
	static  {	// SQL Server types -> JDBC Types (java.sql.Types)   
		TYPE_MAPPING = new HashMap<String,Integer>();
		TYPE_MAPPING.put("varchar", java.sql.Types.VARCHAR);
		TYPE_MAPPING.put("int", java.sql.Types.INTEGER);
		TYPE_MAPPING.put("bit", java.sql.Types.BIT);
		TYPE_MAPPING.put("date", java.sql.Types.DATE);
		TYPE_MAPPING.put("time", java.sql.Types.TIME);
		TYPE_MAPPING.put("datetime", java.sql.Types.TIMESTAMP);
		TYPE_MAPPING.put("smalldatetime", java.sql.Types.TIMESTAMP);
		TYPE_MAPPING.put("tinyint", java.sql.Types.TINYINT);
	}
		
	private static final Logger LOG = Logger.getLogger(TestCDTAnyDB.class);
	private final SmartServiceContext smartServiceCtx;
	private Connection conn;
	private String dataSourceName;
	private AnyCDTs[] objs;


	@Override
	public void run() throws SmartServiceException {
		
		DataSource ds = null;
		Statement stmt = null;
		
		LOG.setLevel(Level.DEBUG);
		LOG.debug("Calling AnyCDT plug in --------------------------------------------------");
		
		if (this.objs == null || this.objs.length == 0) {
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable("Input CDT is null")).build();
		}
		
		try{		
			//ds = (DataSource) this.initialCtx.lookup(this.dataSourceName);
			//conn = ds.getConnection();
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(connectionUrl);
	        conn.setAutoCommit(false);
	        for (AnyCDTs cdt : objs) {
				this.processCDT(conn, cdt);
			}
			
	        conn.commit();
			
	        LOG.debug("Processed AnyCDTs number " + this.objs.length + " has finished Successfully --------------------------------");
	        
			/*
			org.jboss.jca.adapters.jdbc.jdk6.WrappedConnectionJDK6 wrappedConn = ( org.jboss.jca.adapters.jdbc.jdk6.WrappedConnectionJDK6)this.conn; 
			Connection con = wrappedConn.getUnderlyingConnection();
			SQLServerConnection sqlConn = (SQLServerConnection) con; */
			
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" Can't find JDBC driver class ")).build();
		}  catch (SQLException e) {
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" Can't get database connection ")).build();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, e).build();
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
				throw new SmartServiceException.Builder(TestCDTAnyDB.class, e).build();
			}
		} 

	}

	public TestCDTAnyDB(SmartServiceContext smartServiceCtx) {
		super();
		this.smartServiceCtx = smartServiceCtx;
	}

	public void onSave(MessageContainer messages) {
	}

	public void validate(MessageContainer messages) {
	}

	@Input(required = Required.ALWAYS)
	@Name("dataSourceName")
	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}
		
	@Input(required = Required.ALWAYS)
	@Name("cdt")
	public void setObj(AnyCDTs[] anyCDT) {
		this.objs = anyCDT;
	}
	
	/*
	 * This is a helper method only called at log debug level, converting a DOM element to string.
	 */
	private String printElementString(Element ele ) throws SmartServiceException {
		
		try{
			TransformerFactory transFactory = TransformerFactory.newInstance();
			Transformer transformer = transFactory.newTransformer();
			StringWriter buffer = new StringWriter();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.transform(new DOMSource(ele), new StreamResult(buffer));
			String str = buffer.toString();
			return str;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, e).build();
		} 
		
	}
	
	
	/*
	 * Get meta data of user defined table from database and map it to corresponding java.sql.type.
	 */
	private Map<String, Integer> getUserTableTypeMetaData(Connection conn, String name) throws SQLException{
		
		Map<String, Integer> colNameType = new HashMap<String, Integer>();
		PreparedStatement preparedStatement = conn.prepareStatement(TestCDTAnyDB.USER_TABLE_TYPE_QUERY);
		preparedStatement.setString(1, name);
		ResultSet rs = preparedStatement.executeQuery();

	    while (rs.next()) {
	        String colName = rs.getString("name");
	        String colType = rs.getString("type");
	        if (TYPE_MAPPING.get(colType) == null ) {
	        	LOG.error("SQL Server type " + colType + " hasn't been mapped in JDBC types ");
	        	//throw new Exception("wsef");
	        }
	        colNameType.put(colName, TYPE_MAPPING.get(colType));
	    }
	    
	    return colNameType;	      
	}
	
	/*
	 * This method processes each cdt
	 */
	private void processCDT(Connection conn, AnyCDTs anyCDT) throws SmartServiceException {
		
		String userType = anyCDT.getUserDefinedTableType();
		String storedProcedure = anyCDT.getStoredProcedure();
		Object anyObj = anyCDT.getAnyCDT();
		Map<String, Integer>  nameType = null;
		List<String> nodeNames =  new ArrayList<String>();
		SQLServerDataTable userTable = null;
		
		
		if (userType == null || userType.equalsIgnoreCase("")
			|| storedProcedure == null || storedProcedure.equalsIgnoreCase("")) {
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable("userType " + userType + " or storedProcedure " + storedProcedure + " is Invalid" )).build();
		}
		
		if (anyObj == null ) {
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" cdt value for " + storedProcedure + " is null")).build();
		}
				
		Element ele = null;
		if (anyObj instanceof Element) {
			ele = (Element)anyObj; // each cdt is org.apache.xerces.dom.ElementNSImpl
		} 
		
		
		if (LOG.isDebugEnabled()) {
			String printElement = printElementString(ele);
			LOG.debug("CDT content is ======= " + printElement );			
		}
		
		// is there a better way to get node names ?
		NodeList firstNodes = ele.getFirstChild().getChildNodes();
		for (int i=0; i < firstNodes.getLength(); i++ ) {
			nodeNames.add(firstNodes.item(i).getNodeName());
		}
		
		try {
			nameType = this.getUserTableTypeMetaData(conn, userType);
			userTable = new SQLServerDataTable();
			for (String nodeName : nodeNames) {				
				LOG.debug("column name " + nodeName + " type is " + nameType.get(nodeName).intValue() );
				userTable.addColumnMetadata(nodeName, nameType.get(nodeName).intValue());
			}			
		} catch (SQLException e) {	
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" Can't get and map User-Defined Table type details for " + userType)).build();
		}
		
		
		try {
			
			NodeList nl = ele.getChildNodes();
			int len=nl.getLength(); 
			
			// should be while loop ?
			for (int i=0; i < len; i++) {
				Node n = nl.item(i);
				NodeList nlChild = n.getChildNodes();
				int lenChild = nlChild.getLength();
				Object[] rowValues = new Object[lenChild];
				for (int j=0; j < lenChild; j++) {
					rowValues[j] = nlChild.item(j).getTextContent();
				}
				userTable.addRow(rowValues);
			}
		} catch (SQLServerException e) {
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" Failed to create table in memory for " + userType)).build();
		}
		
		
		String sp = "{call " + storedProcedure + "(?)}";									
		SQLServerPreparedStatement pStmt;
		try {
			pStmt = (SQLServerPreparedStatement)conn.prepareStatement(sp);
			pStmt.setStructured(1, userType, userTable);
			pStmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SmartServiceException.Builder(TestCDTAnyDB.class, new Throwable(" Call Stored Procedure " + storedProcedure + " is failed")).build();
		}								
		
		LOG.debug("Store Procedure " + storedProcedure + " is executed successfully ");
		
	}

}

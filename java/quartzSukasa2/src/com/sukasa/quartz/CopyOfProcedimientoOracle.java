package com.sukasa.quartz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CopyOfProcedimientoOracle {

	@SuppressWarnings("unused")
	public static StringBuilder procesarORACLE(StringBuilder sentencia) {
		Connection conexion = null;
		StringBuilder resultado = new StringBuilder();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conexion = DriverManager.getConnection(
					"jdbc:oracle:thin:@//200.105.234.42:7975/sboddb2.sukasa",
					"PORTAL_CLIENTE", "PCLIENTE");

			
			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();
			resultado.append("QUERYS QUE SE VAN GENERANDO EN ORACLE");
			
			//recupera los codigos de los items de los productos de la página web para insertarlos en las tablas de oracle de sukasa
			List<String> listItemsInsertarOracle =  llenarDatosTablaItems();
			
			if(listItemsInsertarOracle!=null && listItemsInsertarOracle.size()>0){
				String queryTruncate = "truncate table items";
				ResultSet rsQueryTruncate = instruccion.executeQuery(queryTruncate);
				for (String string : listItemsInsertarOracle) {
					instruccion.executeUpdate(string.toString());
				}
			}
			
			//Selecciona el kcodigo, kmarca, kitem,kcantidad,kprecio,kprecio_afil de la tabla kardex de oracle para actualizar el precio, precio sukasa, cantidad de los productos de la página web
			String queryKardexOracle = "select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('INSERT INTO _kardex(KCODIGO, KMARCA, KITEM, KPRECIO, KPRECIO_AFIL, KCANTIDAD, KACTUALIZACION, KNO_LOTE) values(',KCODIGO), ','), KMARCA), ','), KITEM), ','), TO_CHAR(KPRECIO, '99999.99')), ','), TO_CHAR(KPRECIO_AFIL, '99999.99')), ','), KCANTIDAD), ',0,0)') as kardexItems, kcodigo, kmarca, kitem, kprecio, kprecio_afil, kcantidad from kardex  inner join items i on kmarca=i.imarca and kitem =i.iitem where kactualizacion=0 order by kcodigo asc";			
			ResultSet rsQueryKardex = instruccion.executeQuery(queryKardexOracle);

			List<Kardex> kardex = new ArrayList<Kardex>();
			
			while (rsQueryKardex.next()) {
				Kardex productoKardex = new Kardex();
				productoKardex.setKcodigo(rsQueryKardex.getInt("KCODIGO"));
				productoKardex.setKmarca(rsQueryKardex.getInt("KMARCA"));
				productoKardex.setKitem(rsQueryKardex.getInt("KITEM"));
				productoKardex.setCantidad(rsQueryKardex.getInt("KCANTIDAD"));
				productoKardex.setPrecio(rsQueryKardex.getDouble("KPRECIO"));
				productoKardex.setPrecioSukasa(rsQueryKardex.getDouble("KPRECIO_AFIL"));
				productoKardex.setQueryInsertar(rsQueryKardex.getString("kardexItems"));
				kardex.add(productoKardex);
			}
			
			if (kardex.size() > 0) {
				updateKardexMagento(kardex);
				System.out.println("actualizo e inserto en kardex");
			}
			
			
			//Selecciona los campos de la tabla maestro tarjeton de oracle para armar el campo tarjeton para actualizarlo en cada unos de los productos de la página web
			String queryMaestroTarjeton = "select * from maestro_tarjeton where mtprecio>0";
			ResultSet rsQueryMaestroTarjeton = instruccion.executeQuery(queryMaestroTarjeton);
			List<Tarjeton> listTarjeton = new ArrayList<Tarjeton>();
			char comillas='"';
			
			while(rsQueryMaestroTarjeton.next()){
				Tarjeton productoTarjeton = new Tarjeton();
				
				productoTarjeton.setSku(rsQueryMaestroTarjeton.getInt("MTMARCA")+"-"+rsQueryMaestroTarjeton.getInt("MTITEM"));
				
				String tarjeton= new String();
				tarjeton = "<h2></h2><div class="+comillas+"stdCuotas"+comillas+"><div class="+comillas+"formUno"+comillas+"><span class="+comillas+"code"+comillas+"> CODIGO: "+rsQueryMaestroTarjeton.getInt("MTMARCA")+"-"+rsQueryMaestroTarjeton.getInt("MTITEM")+"</span><span class="+comillas+"code"+comillas+"> MODEL: "+rsQueryMaestroTarjeton.getString("MTMODELO")+"</span><p id="+comillas+"PVP"+comillas+">PVP: USD$ "+rsQueryMaestroTarjeton.getInt("MTPRECIO")+".</p>";
				
				String queryDetalleTarjetonResalta = "select * from detalle_tarjeton where DTMARCA="+rsQueryMaestroTarjeton.getInt("MTMARCA")+" and DTITEM="+rsQueryMaestroTarjeton.getInt("MTITEM")+" and DTRESALTA='S'";
				ResultSet rsQueryDetalleTarjetonResalta = instruccion.executeQuery(queryDetalleTarjetonResalta);
				
				String queryDetalleTarjetonNoResalta ="select * from detalle_tarjeton where DTMARCA="+rsQueryMaestroTarjeton.getInt("MTMARCA")+" and DTITEM="+rsQueryMaestroTarjeton.getInt("MTITEM")+" and DTRESALTA='N' ORDER BY DTMARCA, DTITEM, DTNO_CUOTA ASC";
				ResultSet rsQueryDetalleTarjetonNoResalta = instruccion.executeQuery(queryDetalleTarjetonNoResalta);
				
				while(rsQueryDetalleTarjetonResalta.next()){
					tarjeton = tarjeton + "<div class="+comillas+"cuotas"+comillas+"><span class="+comillas+"numCuotas"+comillas+">"+rsQueryDetalleTarjetonResalta.getInt("DTNO_CUOTA")+"</span><span class="+comillas+"palCuotas"+comillas+"> cuotas de </span><span class="+comillas+"usdCuotas"+comillas+">"+rsQueryDetalleTarjetonResalta.getDouble("DTVALOR_CUOTAS")+"<sup style="+comillas+"font-size:18px;"+comillas+"></sup></span></div>"; 
				}
				
				tarjeton = tarjeton + "</div><p class="+comillas+"code"+comillas+">"+rsQueryMaestroTarjeton.getInt("MTNOMBRE_PRECIO_OFERTA")+" $ "+rsQueryMaestroTarjeton.getInt("MTPRECIO_OFERTA")+"</p><p class="+comillas+"code"+comillas+">"+rsQueryMaestroTarjeton.getInt("MTDESCRIPCION_PROMO1")+"</p><p class="+comillas+"code"+comillas+"><p class="+comillas+"code"+comillas+">"+rsQueryMaestroTarjeton.getInt("MTDESCRIPCION_PROMO2")+"</p><br/><p class="+comillas+"code"+comillas+">Cuotas:</p><table class="+comillas+"Tabcuotas"+comillas+"><tr>";
				
				while(rsQueryDetalleTarjetonNoResalta.next()){
					tarjeton = tarjeton + "<td><p>"+rsQueryDetalleTarjetonNoResalta.getInt("DTNO_CUOTA")+"  MESES</p><p style="+comillas+"font-size:14px"+comillas+"> X US $ "+rsQueryDetalleTarjetonNoResalta.getDouble("DTVALOR_CUOTAS")+" </p><p style="+comillas+"18px"+comillas+">"+rsQueryDetalleTarjetonNoResalta.getInt("DTNO_CUOTA")*rsQueryDetalleTarjetonNoResalta.getDouble("DTVALOR_CUOTAS")+"</p></td>"; 
				}
				
				tarjeton = tarjeton +"</tr></table><p class="+comillas+"code"+comillas+">Extensi&oacute;n de Garant&iacute;a Original: $127.34</p><p class="+comillas+"legales"+comillas+">PVP incluye IVA. Los diferidos se calculan del PVP. Aplican planes y condiciones de financiamiento vigentes. Diferidos exclusivos para socios SUKASA.</p></div>";
				
				productoTarjeton.setAtributoTarjeton(tarjeton);
				
				listTarjeton.add(productoTarjeton);
				
			}
			
			if(listTarjeton.size()>0){
				updateTarjetonMagento(listTarjeton);
				System.out.println("actualizo e inserto en tarjeton");
			}
			
			// consultas a bdd de oracle para generar los scripts a insertar en
			// las tablas temporales de MySQL
			// Scripts para insertar la información de la tabla Tarjeton

			// String queryTarjeton =
			// "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat( concat(concat(concat(concat(concat('INSERT INTO _tarjeton (TCODIGO, TMARCA, TITEM, TNO_CUOTAS, TVALOR_CUOTAS,TFECHA_DESDE, TFECHA_HASTA, TACTUALIZACION, TNO_LOTE ) values (', TCODIGO),','),TMARCA),','), TITEM), ','), TNO_CUOTAS), ','), TO_CHAR(TVALOR_CUOTAS, '999.99')), ','''),nvl(to_char(TFECHA_DESDE,'YYYY-MM-DD'),'')), ''','''), to_char(TFECHA_HASTA,'YYYY-MM-DD')), ''','), 0), ','), 0), ')') as tarjetonItems from tarjeton  inner join items i on TMARCA=i.imarca and TITEM =i.iitem where tactualizacion=0 or tactualizacion is null";
			/*String queryTarjeton = "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat( concat(concat(concat(concat(concat('INSERT INTO _tarjeton (TCODIGO, TMARCA, TITEM, TNO_CUOTAS, TVALOR_CUOTAS,TFECHA_DESDE, TFECHA_HASTA, TACTUALIZACION, TNO_LOTE ) values (', TCODIGO), ','),TMARCA), ','), TITEM), ','), TNO_CUOTAS), ','), TO_CHAR(TVALOR_CUOTAS, '999.99')), ','''),nvl(TFECHA_DESDE,'')), ''','''), TFECHA_HASTA), ''','), 0), ','), 0), ');') as tarjetonItems, TCODIGO from tarjeton  inner join items i on TMARCA=i.imarca and TITEM =i.iitem";
			ResultSet rsQueryTarjeton = instruccion.executeQuery(queryTarjeton);

			List<String> listTarjeton = new ArrayList<String>();
			List<String> listTarjetonCods = new ArrayList<String>();

			while (rsQueryTarjeton.next()) {
				listTarjeton.add(rsQueryTarjeton.getString("tarjetonItems"));
				listTarjetonCods.add(rsQueryTarjeton.getString("TCODIGO"));
			}

			if (listTarjeton != null && listTarjeton.size() > 0) {
				insertScriptsOracle(listTarjeton, "tarjeton", listTarjetonCods);
			}*/

			// Scripts para insertar la información de la tabla Detalle Tarjeton

			// String queryDetalleTarjeton =
			// "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat('INSERT INTO _detalle_tarjeton(D_CODIGO, D_MARCA,D_ITEM, D_LUGAR, D_CUOTA_RESALTAR, D_FECHA_DESDE, D_FECHA_HASTA, D_NUMERO_CUOTA, D_VALOR_CUOTA, D_ACTUALIZACION,D_NUMERO_LOTE) values(',D_CODIGO), ','), D_MARCA), ','),D_ITEM), ','), D_LUGAR), ','''), D_CUOTA_RESALTAR), ''','''), to_char(D_FECHA_DESDE,'yyyy-MM-dd')), ''','''), to_char(D_FECHA_HASTA,'yyyy-MM-dd')), ''','), nvl(D_NUMERO_CUOTA,'0')), ','), nvl(D_VALOR_CUOTA,'0')), ','), 0), ','),0), ')') as detalleItems, D_CODIGO from detalle_tarjeton inner join items i on D_MARCA=i.imarca and D_ITEM =i.iitem where d_actualizacion!=1 or d_actualizacion is null";
			/*String queryDetalleTarjeton = "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat('INSERT INTO _detalle_tarjeton(D_CODIGO, D_MARCA,D_ITEM, D_LUGAR, D_CUOTA_RESALTAR, D_FECHA_DESDE, D_FECHA_HASTA, D_NUMERO_CUOTA, D_VALOR_CUOTA, D_ACTUALIZACION,D_NUMERO_LOTE) values(',DTCODIGO), ','), DTMARCA), ','),DTITEM), ','), DTLOCAL), ','''), DTRESALTA), ''','''), to_char(DTFECHA_DESDE,'yyyy-MM-dd')), ''','''), to_char(DTFECHA_HASTA,'yyyy-MM-dd')), ''','), nvl(DTNO_CUOTA,'0')), ','), TO_CHAR(DTVALOR_CUOTAS, '999.99')), ','), 0), ','),0), ');') as detalleItems, DTCODIGO from detalle_tarjeton inner join items i on DTMARCA=i.imarca and DTITEM =i.iitem";
			ResultSet rsQueryDetalleTarjeton = instruccion
					.executeQuery(queryDetalleTarjeton);

			List<String> listDetalleTarjeton = new ArrayList<String>();
			List<String> listDetalleTarjetonCods = new ArrayList<String>();

			while (rsQueryDetalleTarjeton.next()) {
				listDetalleTarjeton.add(rsQueryDetalleTarjeton
						.getString("detalleItems"));
				listDetalleTarjetonCods.add(rsQueryDetalleTarjeton
						.getString("D_CODIGO"));
			}

			if (listDetalleTarjeton != null && listDetalleTarjeton.size() > 0) {
				insertScriptsOracle(listDetalleTarjeton, "detalleTarjeton",
						listDetalleTarjetonCods);
			}*/

			// Scripts para insertar la información de la tabla Maestro Tarjeton

			// String queryMaestroTarjeton =
			// "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat('INSERT INTO _maestro_tarjeton(CODIGO,MARCA,ITEM,LUGAR,MODELO,VALOR_GARANTIA,LEGALES,PRECIO,NOMBRE_PRECIO_OFERTA,PRECIO_OFERTA,DESCRIPCION_PROMO1,DESCRIPCION_PROMO2,FECHA_DESDE,FECHA_HASTA,IDICADOR_BORRADO,ACTUALIZACION,NUMERO_LOTE) values(',CODIGO), ','),MARCA), ','),ITEM), ','),LUGAR), ','''),NVL(MODELO,'0')), ''','),NVL(VALOR_GARANTIA,'0')), ','''),LEGALES), ''','),nvl(PRECIO,'0')), ','''),NOMBRE_PRECIO_OFERTA), ''',')"
			// +
			// ",nvl(PRECIO_OFERTA,'0')), ','''),DESCRIPCION_PROMO1), ''','''),DESCRIPCION_PROMO2), ''','''),to_char(FECHA_DESDE,'YYYY-MM-DD')), ''','''),to_char(FECHA_HASTA,'YYYY-MM-DD')), ''','),IDICADOR_BORRADO), ',0,0)') as maestroItems from maestro_tarjeton inner join items i on maestro_tarjeton.marca=i.imarca and maestro_tarjeton.item =i.iitem where actualizacion!=1 or actualizacion is null";

			/*String queryMaestroTarjeton = "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat(concat('INSERT INTO _maestro_tarjeton(CODIGO,MARCA,ITEM,LUGAR,MODELO,VALOR_GARANTIA,LEGALES,PRECIO,NOMBRE_PRECIO_OFERTA,PRECIO_OFERTA,DESCRIPCION_PROMO1,DESCRIPCION_PROMO2,FECHA_DESDE,FECHA_HASTA,IDICADOR_BORRADO,ACTUALIZACION,NUMERO_LOTE) values(',MTCODIGO), ','),MTMARCA), ','),MTITEM), ','),MTLOCAL), ','''),NVL(MTMODELO,'0')), ''','),TO_CHAR(MTVALOR_GARANTIA, '99999.99')), ','''),MTLEGALES), ''','),TO_CHAR(MTPRECIO, '9999.99')), ','''),MTNOMBRE_PRECIO_OFERTA), ''','),TO_CHAR(MTPRECIO_OFERTA, '9999.99')), ','''),MTDESCRIPCION_PROMO1), ''','''),MTDESCRIPCION_PROMO2), ''','''),MTFECHA_DESDE), ''','''),MTFECHA_HASTA), ''','),MTIDICADOR_BORRADO), ',0,0);') as maestroItems from maestro_tarjeton inner join items i on maestro_tarjeton.mtmarca=i.imarca and maestro_tarjeton.mtitem =i.iitem";

			ResultSet rsQueryMaestroTarjeton = instruccion
					.executeQuery(queryMaestroTarjeton);

			List<String> listMaestroTarjeton = new ArrayList<String>();
			List<String> listMaestroTarjetonCods = new ArrayList<String>();

			while (rsQueryMaestroTarjeton.next()) {
				listMaestroTarjeton.add(rsQueryMaestroTarjeton
						.getString("maestroItems"));
				listMaestroTarjetonCods.add(rsQueryDetalleTarjeton
						.getString("MTCODIGO"));
			}

			if (listMaestroTarjeton != null && listMaestroTarjeton.size() > 0) {
				System.out.println("TOTAL MAESTRO TARJETON:"
						+ listMaestroTarjeton.size());
				insertScriptsOracle(listMaestroTarjeton, "maestroTarjeton",
						listMaestroTarjetonCods);
			}*/

			/*
			 * String queryCuotas =
			 * "select concat(concat(concat(concat(concat(concat(concat(concat(concat(concat('INSERT INTO _CUOTAS(CCODIGO,CMARCA,CITEM,CPLAZO,CCUOTA,CACTUALIZACION,CNO_LOTE) VALUES(',CCODIGO), ','),CMARCA), ','),CITEM), ','),CPLAZO), ','),CCUOTA), ',0,0)') as cuotasItems from cuotas inner join items i on cmarca=i.imarca and citem =i.iitem where cactualizacion!=1 or cactualizacion is null"
			 * ;
			 * 
			 * ResultSet rsQueryCuotas = instruccion.executeQuery(queryCuotas);
			 * 
			 * List<String> listCuotas = new ArrayList<String>();
			 * 
			 * while (rsQueryCuotas.next()) {
			 * listCuotas.add(rsQueryCuotas.getString("cuotasItems")); }
			 * 
			 * if (listCuotas != null || listCuotas.size() > 0) {
			 * //insertScriptsOracle(listCuotas, "cuotas"); }
			 */

			/*
			 * String queryDestacados =
			 * "select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('INSERT INTO _DESTACADOS(DCODIGO, DMARCA, DITEM, DFECHA_DESDE, DFECHA_HASTA, DACTUALIZACION, DNO_LOTE ) VALUES(',DCODIGO), ','), DMARCA), ','), DITEM), ','''), to_char(DFECHA_DESDE,'YYYY-MM-DD')), ''','''), to_char(DFECHA_HASTA,'YYYY-MM-DD')), ''',0,0)') as destacadosItems from destacados  inner join items i on dmarca=i.imarca and ditem =i.iitem where dactualizacion!=1 or dactualizacion is null"
			 * ;
			 * 
			 * ResultSet rsQueryDestacados = instruccion
			 * .executeQuery(queryDestacados);
			 * 
			 * List<String> listDestacados = new ArrayList<String>();
			 * 
			 * while (rsQueryDestacados.next()) {
			 * listDestacados.add(rsQueryDestacados
			 * .getString("destacadosItems")); }
			 * 
			 * if (listDestacados != null || listDestacados.size() > 0) {
			 * //insertScriptsOracle(listDestacados, "destacados"); }
			 */

			
		

		} catch (SQLException e) {
			System.out.println("Error de ORACLE: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return resultado;
	}

	
	
	public static void updateKardexMagento(List<Kardex> listScriptInserts) {
		Connection conexion = null;
		StringBuilder resultado = new StringBuilder();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();
			
				for (Kardex string : listScriptInserts) {
					instruccion
					.executeUpdate("update catalog_product_entity_decimal set value="+string.getPrecio()+" where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"+string.getKmarca()+"-"+string.getKitem()+"' and attribute_id =64)");
					instruccion
					.executeUpdate("update catalog_product_entity_decimal set value="+string.getPrecioSukasa()+" where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"+string.getKmarca()+"-"+string.getKitem()+"' and attribute_id =65)");
					instruccion
					.executeUpdate("update cataloginventory_stock_item set qty="+string.getCantidad()+" where product_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"+string.getKmarca()+"-"+string.getKitem()+"')");
				}
			resultado.append("QUERYS QUE SE VAN GENERANDO EN MYSQL");
			System.out.println("Actualizo precio, precio sukasa y cantidad correctamente");
		} catch (SQLException e) {
			System.out.println("Error de MySQL: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void updateTarjetonMagento(List<Tarjeton> listTarjeton) {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();
			
				for (Tarjeton string : listTarjeton) {
					ResultSet rsQuery=instruccion.executeQuery("select * from catalog_product_entity_text where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = "+string.getSku()+") AND attribute_id =165)");
					if(!rsQuery.equals(null)||!rsQuery.equals("")){
						instruccion.executeUpdate("update catalog_product_entity_text set value="+string.getAtributoTarjeton()+" where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = "+string.getSku()+") AND attribute_id =165");
					}else{
						instruccion.executeUpdate("insert into catalog_product_entity_text(entity_type_id,attribute_id, store_id, entity_id, value) values(4, 165, 0, (SELECT entity_id FROM catalog_product_entity WHERE sku = "+string.getSku()+"), "+string.getAtributoTarjeton()+")");
					}
				}
			System.out.println("Actualizo atributos de tarjeton correctamente");
		} catch (SQLException e) {
			System.out.println("Error de MySQL: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void insertScripts(List<String> listScriptInserts,
			String tabla, List<String> listUpdatesCods) {
		Connection conexion = null;
		StringBuilder resultado = new StringBuilder();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
							"root", "yagehosting");
			// DriverManager.getConnection(url, info);
			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();

			if (tabla.equals("tarjeton")) {
				int i = 0;
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
					/*instruccion
							.executeUpdate("UPDATE _tarjeton SET TACTUALIZACION=0 WHERE T_CODIGO="
									+ listUpdatesCods.get(i).toString());
					i++;*/
				}

				updateTablasSukasa("tarjeton", listUpdatesCods);

			} else if (tabla.equals("detalleTarjeton")) {
				int i = 0;
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
					instruccion
							.executeUpdate("UPDATE detalle_tarjeton SET d_actualizacion=0 WHERE d_codigo="
									+ listUpdatesCods.get(i).toString());
					i++;
				}
			} else if (tabla.equals("maestroTarjeton")) {
				int i = 0;
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
					System.out.println("TOTAL INSERTS:" + i++);
				}
			} else if (tabla.equals("cuotas")) {
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
				}
			} else if (tabla.equals("destacados")) {
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
				}
			} else if (tabla.equals("kardex")) {
				int i = 0;
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
					instruccion
					.executeUpdate("UPDATE _kardex SET KACTUALIZACION=1 WHERE KCODIGO="
							+ listUpdatesCods.get(i).toString());
					i++;
				}
			} else if (tabla.equals("mandatorio")) {
				for (String string : listScriptInserts) {
					instruccion.executeUpdate(string.toString());
				}
			}
			// Un objeto ResultSet, almacena los datos de resultados de una
			// consulta
			// ResultSet resultadoQuery =
			// instruccion.executeQuery("SELECT cod , nombre FROM datos");
			resultado.append("QUERYS QUE SE VAN GENERANDO EN MYSQL");
			//System.out.println(conexion);
			System.out.println("Inserto y actualizo en _kardex");
		} catch (SQLException e) {
			System.out.println("Error de MySQL: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static List<String> llenarDatosTablaItems() {

		Connection conexion = null;
		List<String> listInsertsItemsOracle = new ArrayList<String>();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
							"root", "");
			// DriverManager.getConnection(url, info);
			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();

			String queryItems = "SELECT cpe.sku, SUBSTRING_INDEX(cpe.sku,'-',1) AS marca, SUBSTRING_INDEX(cpe.sku,'-',-1) AS item,cpet.value FROM catalog_product_entity cpe, catalog_product_entity_text cpet where cpe.entity_id=cpet.entity_id and cpet.attribute_id=62 and cpe.sku not like 'receta%' and cpe.sku not like 'secre%'";

			ResultSet rsQueryItems = instruccion.executeQuery(queryItems);
			

			while (rsQueryItems.next()) {
				listInsertsItemsOracle
						.add("INSERT INTO items (IMARCA, IITEM) VALUES ("
								+ rsQueryItems.getString("marca") + ","
								+ rsQueryItems.getString("item") + ")");
			}


		} catch (SQLException e) {
			System.out.println("Error de MySQL: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return listInsertsItemsOracle;

	}

	@SuppressWarnings("unused")
	public static void updateTablasSukasa(String tabla,
			List<String> listScriptCodesUpdates) {

		Connection conexion = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conexion = DriverManager.getConnection(
					"jdbc:oracle:thin:@//200.105.234.42:7975/sboddb2.sukasa",
					"PORTAL_CLIENTE", "PCLIENTE");

			Statement instruccion = conexion.createStatement();

			if (tabla.equals("kardex")) {
				int i = 0;
				for (String string : listScriptCodesUpdates) {
					instruccion
					.executeUpdate("UPDATE kardex SET KACTUALIZACION=1 WHERE KCODIGO="
							+ listScriptCodesUpdates.get(i).toString());
					i++;
				}
			}

			System.out.println("Inserto y actualizo en kardex");
		} catch (SQLException e) {
			System.out.println("Error de Oracle: " + e.getMessage());
		} catch (Exception e) {
			System.out.println("Error inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}
	

}

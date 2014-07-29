package com.sukasa.quartz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Copy_2_of_ProcedimientoOracle {

	public static StringBuilder procesarORACLE(StringBuilder sentencia) {
		Connection conexion = null;
		StringBuilder resultado = new StringBuilder();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conexion = DriverManager.getConnection(
					"jdbc:oracle:thin:@//200.105.234.42:7975/sboddb.sukasa",
					"PORTAL_CLIENTE", "PCLIENTE");

			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();
			resultado.append("PROCESOS EN TABLAS DE ORACLE");

			// recupera los codigos de los items de los productos de la página
			// web para insertarlos en las tablas de oracle de sukasa
			/*List<String> listItemsInsertarOracle = llenarDatosTablaItems();

			if (listItemsInsertarOracle != null
					&& listItemsInsertarOracle.size() > 0) {
				String queryTruncate = "truncate table items";
				instruccion.executeQuery(queryTruncate);
				for (String string : listItemsInsertarOracle) {
					instruccion.executeUpdate(string.toString());
				}
				System.out
						.println("Actualizo e inserto productos en tabla items de sukasa");
			}*/

			//ocultarProductosCantidadCero();
			
			// Selecciona el kcodigo, kmarca,
			// kitem,kcantidad,kprecio,kprecio_afil de la tabla kardex de oracle
			// para actualizar el precio, precio sukasa, cantidad de los
			// productos de la página web
			/*String queryKardexOracle = "select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('INSERT INTO _kardex(KCODIGO, KMARCA, KITEM, KPRECIO, KPRECIO_AFIL, KCANTIDAD, KACTUALIZACION, KNO_LOTE) values(',KCODIGO), ','), KMARCA), ','), KITEM), ','), TO_CHAR(KPRECIO, '99999.99')), ','), TO_CHAR(KPRECIO_AFIL, '99999.99')), ','), KCANTIDAD), ',0,0)') as kardexItems, kcodigo, kmarca, kitem, kprecio, kprecio_afil, kcantidad from kardex  inner join items i on kmarca=i.imarca and kitem =i.iitem where kactualizacion=0 order by kcodigo asc";
			ResultSet rsQueryKardex = instruccion
					.executeQuery(queryKardexOracle);

			List<Kardex> kardex = new ArrayList<Kardex>();

			while (rsQueryKardex.next()) {
				Kardex productoKardex = new Kardex();
				productoKardex.setKcodigo(rsQueryKardex.getInt("KCODIGO"));
				productoKardex.setKmarca(rsQueryKardex.getInt("KMARCA"));
				productoKardex.setKitem(rsQueryKardex.getInt("KITEM"));
				productoKardex.setCantidad(rsQueryKardex.getInt("KCANTIDAD"));
				productoKardex.setPrecio(rsQueryKardex.getDouble("KPRECIO"));
				productoKardex.setPrecioSukasa(rsQueryKardex
						.getDouble("KPRECIO_AFIL"));
				productoKardex.setQueryInsertar(rsQueryKardex
						.getString("kardexItems"));
				kardex.add(productoKardex);
			}

			if (kardex.size() > 0) {
				System.out.println("TOTAL KARDEX:" + kardex.size());
				updateKardexMagento(kardex);
			}

			// Scripts para insertar la información de la tabla temporal
			// _maestro_tarjeton

			String queryMaestroTarjeton = "select * from maestro_tarjeton";

			ResultSet rsQueryMaestroTarjeton = instruccion
					.executeQuery(queryMaestroTarjeton);

			List<MaestroTarjeton> listMaestroTarjeton = new ArrayList<MaestroTarjeton>();

			while (rsQueryMaestroTarjeton.next()) {
				MaestroTarjeton maestroTarjeton = new MaestroTarjeton();
				
				
				maestroTarjeton.setMtCodigo(rsQueryMaestroTarjeton
						.getInt("MTMARCA"));
				maestroTarjeton.setMtItem(rsQueryMaestroTarjeton
						.getInt("MTITEM"));
				maestroTarjeton.setMtModelo(rsQueryMaestroTarjeton
						.getString("MTMODELO"));
				maestroTarjeton.setMtValorGarantia(rsQueryMaestroTarjeton
						.getDouble("MTVALOR_GARANTIA"));
				maestroTarjeton.setMtLegales(rsQueryMaestroTarjeton
						.getString("MTLEGALES"));
				maestroTarjeton.setMtPrecio(rsQueryMaestroTarjeton
						.getDouble("MTPRECIO"));
				String mtnombre_marca = new String();
				mtnombre_marca = rsQueryMaestroTarjeton
						.getString("MTNOMBRE_MARCA");
				mtnombre_marca = mtnombre_marca.replaceAll("'",
						"&nbsp;");
				maestroTarjeton.setMtNombreMarca(mtnombre_marca);
				maestroTarjeton.setMtPrecioOferta(rsQueryMaestroTarjeton
						.getDouble("MTPRECIO_OFERTA"));
				if (rsQueryMaestroTarjeton.getString("MTDESCRIPCION_PROMO1") == null) {
					maestroTarjeton.setMtDescripcionPromo1("");
				} else {
					maestroTarjeton
							.setMtDescripcionPromo1(rsQueryMaestroTarjeton
									.getString("MTDESCRIPCION_PROMO1"));
				}

				if (rsQueryMaestroTarjeton.getDate("MTFECHA_DESDE") == null) {
					String fechaDesde = "1999/6/6";
					maestroTarjeton.setMtFechaDesde(fechaDesde);
				} else {
					String fechaDesde = rsQueryMaestroTarjeton.getDate(
							"MTFECHA_DESDE").toString();
					maestroTarjeton.setMtFechaDesde(fechaDesde);
				}

				if (rsQueryMaestroTarjeton.getDate("MTFECHA_HASTA") == null) {
					String fechaHasta = "1999/6/6";
					maestroTarjeton.setMtFechaHasta(fechaHasta);
				} else {
					String fechaHasta = rsQueryMaestroTarjeton.getDate(
							"MTFECHA_HASTA").toString();
					maestroTarjeton.setMtFechaHasta(fechaHasta);
				}

				maestroTarjeton.setMtPromocion(rsQueryMaestroTarjeton
						.getInt("MT_PROMOCION"));

				listMaestroTarjeton.add(maestroTarjeton);
			}

			if (listMaestroTarjeton.size() > 0) {
				System.out.println("TOTAL MAESTRO TARJETON:"
						+ listMaestroTarjeton.size());
				insertScripts(listMaestroTarjeton);
			}*/

			// Recolecta información para calcular las cuotas mensuales de los
			// productos en el tarjeton del portal de las tablas param_tarjeton,
			// promocion y fpmes de sukasa
			String queryParamTarjeton = "select * from param_tarjeton";
			ResultSet rsQueryParamTarjeton = instruccion
					.executeQuery(queryParamTarjeton);
			List<ParamTarjeton> listParamTarjeton = new ArrayList<ParamTarjeton>();

			while (rsQueryParamTarjeton.next()) {
				ParamTarjeton paramTarjeton = new ParamTarjeton();
				paramTarjeton.setPt_grupo(rsQueryParamTarjeton
						.getInt("PT_GRUPO"));
				paramTarjeton.setPt_nmeses(rsQueryParamTarjeton
						.getInt("PT_NMESES"));
				paramTarjeton
						.setPt_pvpi(rsQueryParamTarjeton.getInt("PT_PVPI"));
				paramTarjeton
						.setPt_pvpf(rsQueryParamTarjeton.getInt("PT_PVPF"));
				listParamTarjeton.add(paramTarjeton);
			}

			String queryPromocion = "select * from promocion order by pcod asc";
			ResultSet rsQueryPromocion = instruccion
					.executeQuery(queryPromocion);
			List<Promocion> listPromocion = new ArrayList<Promocion>();

			while (rsQueryPromocion.next()) {
				Promocion promocion = new Promocion();
				promocion.setPcod(rsQueryPromocion.getInt("PCOD"));
				promocion.setPformapago(rsQueryPromocion.getInt("PFORMAPAGO"));
				promocion.setPtipo(rsQueryPromocion.getInt("PTIPO"));
				promocion.setPmes(rsQueryPromocion.getInt("PMES"));
				promocion.setPfactor(rsQueryPromocion.getInt("PFACTOR"));
				listPromocion.add(promocion);
			}

			String queryFpmes = "select * from fpmes order by mmes asc";
			ResultSet rsQueryFpmes = instruccion.executeQuery(queryFpmes);
			List<Fpmes> listFpmes = new ArrayList<Fpmes>();

			while (rsQueryFpmes.next()) {
				Fpmes fpmes = new Fpmes();
				fpmes.setMmes(rsQueryFpmes.getInt("MMES"));
				fpmes.setMdesc(rsQueryFpmes.getInt("MDESC"));
				listFpmes.add(fpmes);
			}
			
			String queryPlazoMonto = "select * from plazo_monto";
			ResultSet rsQueryPlazoMonto = instruccion.executeQuery(queryPlazoMonto);
			List<PlazoMonto> listPlazoMonto = new ArrayList<PlazoMonto>();
			
			while(rsQueryPlazoMonto.next()){
				PlazoMonto plazoMonto = new PlazoMonto();
				plazoMonto.setMes(rsQueryPlazoMonto.getInt("MES"));
				plazoMonto.setMontoMinimo(rsQueryPlazoMonto.getDouble("MONTO_MINIMO"));
				listPlazoMonto.add(plazoMonto);
			}
			

			// función que actualiza el atributo tarjeton de los productos de la
			// página web
			if (listParamTarjeton.size() > 0 && listPromocion.size() > 0
					&& listFpmes.size() > 0 && listPlazoMonto.size()>0) {
				actualizarTarjetonProductos(listParamTarjeton, listPromocion,
						listFpmes,listPlazoMonto);
			}

			// Creacion un objeto lista de productos destados web para
			// insertarlos en la tabla _destacados_web

			String queryDestacadosWeb = "select * from destacados_web where dmarca is not null";

			ResultSet rsQueryDestacadosWeb = instruccion
					.executeQuery(queryDestacadosWeb);

			List<DestacadosWeb> listDestacadosWeb = new ArrayList<DestacadosWeb>();

			while (rsQueryDestacadosWeb.next()) {
				DestacadosWeb prodDescatado = new DestacadosWeb();
				prodDescatado.setDmarca(rsQueryDestacadosWeb.getInt("DMARCA"));
				prodDescatado.setDitem(rsQueryDestacadosWeb.getInt("DITEM"));
				prodDescatado.setDcategoria(rsQueryDestacadosWeb
						.getString("DCATEGORIA"));
				DateFormat dateFormat = new SimpleDateFormat("yyyy/mm/dd");
				Date dfechai = rsQueryDestacadosWeb.getDate("DFECHAI");
				dateFormat.format(dfechai);
				Date dfechaf = rsQueryDestacadosWeb.getDate("DFECHAF");
				dateFormat.format(dfechaf);
				Date dfecha_ingreso = rsQueryDestacadosWeb
						.getDate("DFECHA_INGRESO");
				dateFormat.format(dfecha_ingreso);
				prodDescatado.setDfechai(dfechai);
				prodDescatado.setDfechaf(dfechaf);
				prodDescatado.setDfecha_ingreso(dfecha_ingreso);
				prodDescatado.setDdestacado(rsQueryDestacadosWeb
						.getString("DDESTACADO"));
				listDestacadosWeb.add(prodDescatado);
			}

			if (listDestacadosWeb.size() > 0) {
				System.out.println("TOTAL DESTACADOS WEB:"
						+ listDestacadosWeb.size());
				insertDestacadoWeb(listDestacadosWeb);
			}

			// Creacion un objeto lista de promociones web para actualizar los
			// productos que aparecen en la seccion de promociones
			String queryPromocionesWeb = "select * from promocion_web";

			ResultSet rsQueryPromocionesWeb = instruccion
					.executeQuery(queryPromocionesWeb);

			List<PromocionesWeb> listPromocionesWeb = new ArrayList<PromocionesWeb>();

			while (rsQueryPromocionesWeb.next()) {
				PromocionesWeb prodPromocion = new PromocionesWeb();
				prodPromocion.setIlugar(rsQueryPromocionesWeb.getInt("ILUGAR"));
				prodPromocion.setImarca(rsQueryPromocionesWeb.getInt("IMARCA"));
				prodPromocion.setIcod(rsQueryPromocionesWeb.getInt("ICOD"));
				prodPromocion.setIpromocion(rsQueryPromocionesWeb
						.getInt("IPROMOCION"));
				DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
				Date dfecha1 = rsQueryPromocionesWeb.getDate("IFECHA1");
				dateFormat.format(dfecha1);
				Date dfecha2 = rsQueryPromocionesWeb.getDate("IFECHA2");
				dateFormat.format(dfecha2);
				Date dfecha_ingreso = rsQueryPromocionesWeb
						.getDate("IFECHA_INGRESO");
				dateFormat.format(dfecha_ingreso);
				prodPromocion.setIfecha1(dfecha1);
				prodPromocion.setIfecha2(dfecha2);
				prodPromocion.setIfechaIngreso(dfecha_ingreso);
				prodPromocion.setIestado(rsQueryPromocionesWeb
						.getString("IESTADO"));
				prodPromocion.setDestacado(rsQueryPromocionesWeb
						.getInt("DESTACADO"));
				listPromocionesWeb.add(prodPromocion);
			}

			if (listPromocionesWeb.size() > 0) {
				System.out.println("TOTAL PROMOCIONES WEB:"
						+ listPromocionesWeb.size());
				publicarPromocionesWeb(listPromocionesWeb);
			}

			ocultarProductosCantidadCero();
			publicarProductosSinCantidadCero();
			
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
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			for (Kardex string : listScriptInserts) {
				instruccion
						.executeUpdate("update catalog_product_entity_decimal set value="
								+ string.getPrecio()
								+ " where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"
								+ string.getKmarca()
								+ "-"
								+ string.getKitem()
								+ "' and attribute_id =64)");
				instruccion
						.executeUpdate("update catalog_product_entity_decimal set value="
								+ string.getPrecioSukasa()
								+ " where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"
								+ string.getKmarca()
								+ "-"
								+ string.getKitem()
								+ "' and attribute_id =65)");
				instruccion
						.executeUpdate("update cataloginventory_stock_item set qty="
								+ string.getCantidad()
								+ " where product_id=(SELECT entity_id FROM catalog_product_entity WHERE sku='"
								+ string.getKmarca()
								+ "-"
								+ string.getKitem()
								+ "')");
			}
			System.out
					.println("Actualizo precio, precio sukasa y cantidad correctamente en magento");
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

	
	public static void ocultarProductosCantidadCero(){
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			//ResultSet rsQuery = instruccion.executeQuery("SELECT product_id FROM cataloginventory_stock_item WHERE qty =0");
			ResultSet rsQuery = instruccion.executeQuery("SELECT csi.product_id FROM catalog_product_entity cpe, cataloginventory_stock_item csi WHERE cpe.entity_id = csi.product_id AND cpe.sku NOT LIKE 'receta%' AND cpe.sku NOT LIKE 'secre%'AND csi.qty =0");
			while(rsQuery.next()){
				Statement instruccion1 = conexion.createStatement();
				instruccion1.executeUpdate("update catalog_product_entity_int set value=2 where entity_id="+rsQuery.getInt("product_id")+" and  attribute_id =84");
			}

			System.out.println("Se oculto productos con cantidad en stock=0");
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
	
	public static void publicarProductosSinCantidadCero(){
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();
			ResultSet rsQueryPublicar = instruccion.executeQuery("SELECT csi.product_id FROM catalog_product_entity cpe, cataloginventory_stock_item csi WHERE cpe.entity_id = csi.product_id AND cpe.sku NOT LIKE 'receta%' AND cpe.sku NOT LIKE 'secre%'AND csi.qty >0");
			
			while(rsQueryPublicar.next()){
				Statement instruccion1 = conexion.createStatement();
				instruccion1.executeUpdate("update catalog_product_entity_int set value=1 where entity_id="+rsQueryPublicar.getInt("product_id")+" and  attribute_id =84");
			}
				
			System.out.println("Se publico productos con cantidad en stock > 0");
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
			Statement instruccion = conexion.createStatement();

			for (Tarjeton string : listTarjeton) {
				ResultSet rsQuery = instruccion
						.executeQuery("select * from catalog_product_entity_text where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = "
								+ string.getSku() + ") AND attribute_id =165)");
				if (!rsQuery.equals(null) || !rsQuery.equals("")) {
					instruccion
							.executeUpdate("update catalog_product_entity_text set value="
									+ string.getAtributoTarjeton()
									+ " where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = "
									+ string.getSku()
									+ ") AND attribute_id =165");
				} else {
					instruccion
							.executeUpdate("insert into catalog_product_entity_text(entity_type_id,attribute_id, store_id, entity_id, value) values(4, 165, 0, (SELECT entity_id FROM catalog_product_entity WHERE sku = "
									+ string.getSku()
									+ "), "
									+ string.getAtributoTarjeton() + ")");
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

	public static List<String> llenarDatosTablaItems() {

		Connection conexion = null;
		List<String> listInsertsItemsOracle = new ArrayList<String>();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
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

	public static void actualizarTarjetonProductos(
			List<ParamTarjeton> listParamTarjeton,
			List<Promocion> listPromocion, List<Fpmes> listFpmes, List<PlazoMonto>listPlazoMonto) {

		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
							"root", "yagehosting");
			Statement instruccion = conexion.createStatement();
			//String queryMaestroTarjeton = "select * from _maestro_tarjeton where precio>0";
			//String queryMaestroTarjeton = "SELECT * FROM _maestro_tarjeton WHERE codigo=1031 or codigo=158 or codigo=1287 or codigo=1315 or codigo=177";
			String queryMaestroTarjeton = "SELECT * FROM _maestro_tarjeton WHERE codigo=177";
			ResultSet rsQueryMaestroTarjeton = instruccion
					.executeQuery(queryMaestroTarjeton);
			List<Tarjeton> listTarjeton = new ArrayList<Tarjeton>();
			char comillas = '"';
			int grupo = 0;
			//int meses = 0;
			int codPromocionMT = 0;
			// List<DescuentosFpmes> listDescuentosFpmes = new
			// ArrayList<DescuentosFpmes>();

			DecimalFormat df = new DecimalFormat("####.##");

			while (rsQueryMaestroTarjeton.next()) {
				grupo = 0;
				Tarjeton productoTarjeton = new Tarjeton();
				List<DescuentoMesesPromocion> listDescuentoMesesPromocion = new ArrayList<DescuentoMesesPromocion>();
				for (ParamTarjeton paramTarjeton : listParamTarjeton) {
					if (rsQueryMaestroTarjeton.getDouble("PRECIO") >= paramTarjeton
							.getPt_pvpi()
							&& rsQueryMaestroTarjeton.getDouble("PRECIO") <= paramTarjeton
									.getPt_pvpf()) {
						grupo = paramTarjeton.getPt_grupo();
						//meses = paramTarjeton.getPt_nmeses();
						break;
					}
				}
				
				boolean calcularCuota3Meses = false;
				boolean calcularCuota6Meses = false;
				boolean calcularCuota9Meses = false;
				boolean calcularCuota12Meses = false;
				boolean calcularCuota15Meses = false;
				boolean calcularCuota18Meses = false;
				List<Integer> numeroCuotas= new ArrayList<Integer>();
				
				for (PlazoMonto plazoMonto : listPlazoMonto) {
					if(grupo==1){
						if(plazoMonto.getMes()==3 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota3Meses = true;
							numeroCuotas.add(3);
						}
					}else if(grupo==2){
						if(plazoMonto.getMes()==3 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota3Meses = true;
							numeroCuotas.add(3);
						}else if(plazoMonto.getMes()==6 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota6Meses = true;
							numeroCuotas.add(6);
						}else if(plazoMonto.getMes()==9 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota9Meses = true;
							numeroCuotas.add(9);
						}else if(plazoMonto.getMes()==12 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota12Meses = true;
							numeroCuotas.add(12);
						}
						
					}else if(grupo==3){
						if(plazoMonto.getMes()==9 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota9Meses = true;
							numeroCuotas.add(9);
						}else if(plazoMonto.getMes()==12 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota12Meses = true;
							numeroCuotas.add(12);
						}else if(plazoMonto.getMes()==15 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota15Meses = true;
							numeroCuotas.add(15);
						}else if(plazoMonto.getMes()==18 && rsQueryMaestroTarjeton.getDouble("PRECIO")>=plazoMonto.getMontoMinimo()){
							calcularCuota18Meses = true;
							numeroCuotas.add(18);
						}
					}
						
				}

				productoTarjeton.setSku(rsQueryMaestroTarjeton.getInt("MARCA")
						+ "-" + rsQueryMaestroTarjeton.getInt("ITEM"));

				// selecciona el codigo de la promocion para consultar el
				// porcentaje a utilizar en el calculo de las cuotitas
				codPromocionMT = rsQueryMaestroTarjeton.getInt("PROMOCION");

				if (codPromocionMT != 0) {
					if (grupo == 1) {
						for (Promocion promocionlista : listPromocion) {
							if (promocionlista.getPcod() == codPromocionMT) {
								if (promocionlista.getPmes() == 3) {
									DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
									descMesesProm.setDescuento(promocionlista
											.getPfactor());
									descMesesProm.setMeses(promocionlista
											.getPmes());
									listDescuentoMesesPromocion
											.add(descMesesProm);
								}
							}
						}
					} else if (grupo == 2) {
						for (Promocion promocionlista : listPromocion) {
							if (promocionlista.getPcod() == codPromocionMT) {
								if (promocionlista.getPmes() == 3
										|| promocionlista.getPmes() == 6
										|| promocionlista.getPmes() == 9
										|| promocionlista.getPmes() == 12) {
									DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
									descMesesProm.setDescuento(promocionlista
											.getPfactor());
									descMesesProm.setMeses(promocionlista
											.getPmes());
									listDescuentoMesesPromocion
											.add(descMesesProm);
								}
							}
						}
					} else if (grupo == 3) {
						for (Promocion promocionlista : listPromocion) {
							if (promocionlista.getPcod() == codPromocionMT) {
								if (promocionlista.getPmes() == 9
										|| promocionlista.getPmes() == 12
										|| promocionlista.getPmes() == 15
										|| promocionlista.getPmes() == 18) {
									DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
									descMesesProm.setDescuento(promocionlista
											.getPfactor());
									descMesesProm.setMeses(promocionlista
											.getPmes());
									listDescuentoMesesPromocion
											.add(descMesesProm);
								}
							}
						}
					}
				} else {
					if (grupo == 1) {
						for (Fpmes fpmes : listFpmes) {
							if (fpmes.getMmes() == 3) {
								DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
								descMesesProm.setDescuento(fpmes.getMdesc());
								descMesesProm.setMeses(3);
								listDescuentoMesesPromocion.add(descMesesProm);
							}
						}
					} else if (grupo == 2) {
						for (Fpmes fpmes : listFpmes) {
							if (fpmes.getMmes() == 3 || fpmes.getMmes() == 6
									|| fpmes.getMmes() == 9
									|| fpmes.getMmes() == 12) {
								DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
								descMesesProm.setDescuento(fpmes.getMdesc());
								descMesesProm.setMeses(fpmes.getMmes());
								listDescuentoMesesPromocion.add(descMesesProm);
							}
						}
					} else if (grupo == 3) {
						for (Fpmes fpmes : listFpmes) {
							if (fpmes.getMmes() == 9 || fpmes.getMmes() == 12
									|| fpmes.getMmes() == 15
									|| fpmes.getMmes() == 18) {
								DescuentoMesesPromocion descMesesProm = new DescuentoMesesPromocion();
								descMesesProm.setDescuento(fpmes.getMdesc());
								descMesesProm.setMeses(fpmes.getMmes());
								listDescuentoMesesPromocion.add(descMesesProm);
							}
						}
					}
				}

				String cuotasProductoTarjeton = new String();
				String cuotaResaltarProductoTarjeton = new String();

				if (listDescuentoMesesPromocion.size() > 0) {

					for (DescuentoMesesPromocion descuentoMesesPromocion : listDescuentoMesesPromocion) {
						if (descuentoMesesPromocion.getDescuento() == 0) {
							double valorCuota = rsQueryMaestroTarjeton
									.getDouble("PRECIO")
									/ descuentoMesesPromocion.getMeses();
							double valorMensual = valorCuota
									* descuentoMesesPromocion.getMeses();
							
							//if(calcularCuota3Meses==true || calcularCuota12Meses==true || calcularCuota18Meses==true){
								if (descuentoMesesPromocion.getMeses() == 3
										|| descuentoMesesPromocion.getMeses() == 12 || descuentoMesesPromocion.getMeses() == 15
										|| descuentoMesesPromocion.getMeses() == 18) {
									
									if(calcularCuota3Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota3Meses=false;
										
									}else if(calcularCuota12Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota12Meses=false;
										
									}else if(calcularCuota15Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota15Meses=false;
										
									}else if(calcularCuota18Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota18Meses=false;
									}
										
								}
							//}
							
							if (grupo == 1) {
								if(calcularCuota3Meses==true){
									cuotasProductoTarjeton = cuotasProductoTarjeton
											+ "<td><p>"
											+ descuentoMesesPromocion.getMeses()
											+ "  MESES</p><p style=" + comillas
											+ "font-size:14px" + comillas
											+ "> X US $ " + df.format(valorCuota)
											+ " </p><p style=" + comillas + "18px"
											+ comillas + ">$ "
											+ df.format(valorMensual) + "</p></td>";
								}
							} else if (grupo == 2) {
								
								//if(calcularCuota3Meses==true || calcularCuota6Meses==true || calcularCuota9Meses==true || calcularCuota12Meses==true){
									if (descuentoMesesPromocion.getMeses() != 12) {
										
										if(calcularCuota3Meses==true || calcularCuota6Meses==true || calcularCuota9Meses==true){
											cuotasProductoTarjeton = cuotasProductoTarjeton
													+ "<td><p>"
													+ descuentoMesesPromocion
													.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas
													+ "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas
													+ "18px"
													+ comillas
													+ ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
										}
									}
								//}
								

							} else if (grupo == 3) {
								//if(calcularCuota9Meses==true || calcularCuota12Meses==true || calcularCuota15Meses==true || calcularCuota18Meses==true){
									if (descuentoMesesPromocion.getMeses() != 18) {
										
										if(calcularCuota9Meses==true || calcularCuota12Meses==true || calcularCuota15Meses==true){
											cuotasProductoTarjeton = cuotasProductoTarjeton
													+ "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas
													+ "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas
													+ "18px"
													+ comillas
													+ ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
										}
									}
								//}
							}
						} else {
							double factorDescuento = descuentoMesesPromocion
									.getDescuento();
							double factor = (double) factorDescuento / 100;
							double cuota = rsQueryMaestroTarjeton
									.getDouble("PRECIO") * factor;
							double valorMensual = rsQueryMaestroTarjeton
									.getDouble("PRECIO") - cuota;
							double valorCuota = valorMensual
									/ descuentoMesesPromocion.getMeses();
							
							//if(calcularCuota3Meses==true || calcularCuota12Meses==true || calcularCuota18Meses==true){
								if (descuentoMesesPromocion.getMeses() == 3
										|| descuentoMesesPromocion.getMeses() == 12 || descuentoMesesPromocion.getMeses() == 15
										|| descuentoMesesPromocion.getMeses() == 18) {
									if(calcularCuota3Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota3Meses=false;
										
									}else if(calcularCuota12Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota12Meses=false;
										
									}else if(calcularCuota15Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota15Meses=false;
										
									}else if(calcularCuota18Meses==true){
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas + "cuotas" + comillas
												+ "><span class=" + comillas
												+ "numCuotas" + comillas + ">"
												+ descuentoMesesPromocion.getMeses()
												+ "</span><span class=" + comillas
												+ "palCuotas" + comillas
												+ "> cuotas de </span><span class="
												+ comillas + "usdCuotas" + comillas
												+ ">$ " + df.format(valorCuota)
												+ "<sup style=" + comillas
												+ "font-size:18px;" + comillas
												+ "></sup></span></div>";
										calcularCuota18Meses=false;
									}
								}
							//}

							if (grupo == 1) {
								if(calcularCuota3Meses==true){
								cuotasProductoTarjeton = cuotasProductoTarjeton
										+ "<td><p>"
										+ descuentoMesesPromocion.getMeses()
										+ "  MESES</p><p style=" + comillas
										+ "font-size:14px" + comillas
										+ "> X US $ " + df.format(valorCuota)
										+ " </p><p style=" + comillas + "18px"
										+ comillas + ">$ "
										+ df.format(valorMensual) + "</p></td>";
								}
							} else if (grupo == 2) {
								//if(calcularCuota3Meses==true || calcularCuota6Meses==true || calcularCuota9Meses==true || calcularCuota12Meses==true){
									if (descuentoMesesPromocion.getMeses() != 12) {
										if(calcularCuota3Meses==true || calcularCuota6Meses==true || calcularCuota9Meses==true){
										cuotasProductoTarjeton = cuotasProductoTarjeton
												+ "<td><p>"
												+ descuentoMesesPromocion
														.getMeses()
												+ "  MESES</p><p style="
												+ comillas
												+ "font-size:14px"
												+ comillas
												+ "> X US $ "
												+ df.format(valorCuota)
												+ " </p><p style="
												+ comillas
												+ "18px"
												+ comillas
												+ ">$ "
												+ df.format(valorMensual)
												+ "</p></td>";
										}
									}
								//}

							} else if (grupo == 3) {
								//if(calcularCuota9Meses==true || calcularCuota12Meses==true || calcularCuota15Meses==true || calcularCuota18Meses==true){
									if (descuentoMesesPromocion.getMeses() != 18) {
										if(calcularCuota9Meses==true || calcularCuota12Meses==true || calcularCuota15Meses==true){
										cuotasProductoTarjeton = cuotasProductoTarjeton
												+ "<td><p>"
												+ descuentoMesesPromocion
														.getMeses()
												+ "  MESES</p><p style="
												+ comillas
												+ "font-size:14px"
												+ comillas
												+ "> X US $ "
												+ df.format(valorCuota)
												+ " </p><p style="
												+ comillas
												+ "18px"
												+ comillas
												+ ">$ "
												+ df.format(valorMensual)
												+ "</p></td>";
										}
									}
								//}
							}

						}
					}

				}

				String tarjeton = new String();
				String cuotaResaltar = new String();
				// String cuotas = new String();
				tarjeton = "<div class=" + comillas + "stdCuotas" + comillas
						+ "><div class=" + comillas + "formUno" + comillas
						+ "><span class=" + comillas + "code" + comillas
						+ "> CODIGO: " + rsQueryMaestroTarjeton.getInt("MARCA")
						+ "-" + rsQueryMaestroTarjeton.getInt("ITEM")
						+ "</span><span class=" + comillas + "code" + comillas
						+ "> MODELO: "
						+ rsQueryMaestroTarjeton.getString("MODELO")
						+ "</span><p id=" + comillas + "PVP" + comillas
						+ ">PVP: USD$ "
						+ rsQueryMaestroTarjeton.getDouble("PRECIO") + "</p>";

				cuotaResaltar = cuotaResaltarProductoTarjeton;

				tarjeton = tarjeton + cuotaResaltar;

				String fechasPromocion = new String();
				if (!rsQueryMaestroTarjeton.getDate("FECHA_DESDE").toString()
						.equals("1999-06-06")) {
					fechasPromocion = "<p class=" + comillas + "code"
							+ comillas + ">Del: "
							+ rsQueryMaestroTarjeton.getDate("FECHA_DESDE")
							+ " al "
							+ rsQueryMaestroTarjeton.getDate("FECHA_HASTA")
							+ "</p><br/>";
				} else {
					fechasPromocion = "";
				}

				String descripcionPromo = new String();
				descripcionPromo = rsQueryMaestroTarjeton
						.getString("DESCRIPCION_PROMO1");
				descripcionPromo = descripcionPromo.replaceAll("363",
						"&oacute;");
				
				String cuotas = new String();
				if(calcularCuota3Meses==true || calcularCuota6Meses==true || calcularCuota9Meses==true || calcularCuota12Meses==true || calcularCuota15Meses==true || calcularCuota18Meses==true){
					cuotas = "Cuotas:";
				}
				
				tarjeton = tarjeton
						+ "</div><p class="
						+ comillas
						+ "code"
						+ comillas
						+ ">"
						+ rsQueryMaestroTarjeton
								.getString("NOMBRE_PRECIO_OFERTA") + " $ "
						+ rsQueryMaestroTarjeton.getDouble("PRECIO_OFERTA")
						+ "</p><p class=" + comillas + "code" + comillas + ">"
						+ descripcionPromo + "</p>" + fechasPromocion
						+ "<p class=" + comillas + "code" + comillas
						+ "><br/><p class=" + comillas + "code" + comillas
						+ ">"+cuotas+"</p><table class=" + comillas + "Tabcuotas"
						+ comillas + "><tr>";

				// tarjeton = tarjeton + cuotas;
				tarjeton = tarjeton + cuotasProductoTarjeton;

				if(rsQueryMaestroTarjeton.getDouble("VALOR_GARANTIA")>0){
					tarjeton = tarjeton + "</tr></table><p class=" + comillas
							+ "code" + comillas
							+ ">Extensi&oacute;n de Garant&iacute;a Original: $"
							+ rsQueryMaestroTarjeton.getDouble("VALOR_GARANTIA")
							+ "</p><p class=" + comillas + "legales" + comillas
							+ ">" + rsQueryMaestroTarjeton.getString("LEGALES")
							+ "</p></div>";
				}else{
					tarjeton = tarjeton + "</tr></table><p class=" + comillas
							+ "code" + comillas
							+ ">Extensi&oacute;n de Garant&iacute;a Original: No Aplica</p><p class=" + comillas + "legales" + comillas
							+ ">" + rsQueryMaestroTarjeton.getString("LEGALES")
							+ "</p></div>";
					
				}
					
				

				productoTarjeton.setAtributoTarjeton(tarjeton);

				listTarjeton.add(productoTarjeton);

			}

			if (listTarjeton.size() > 0) {
				for (Tarjeton tarjeton : listTarjeton) {
					ResultSet rsQuery = instruccion
							.executeQuery("select * from catalog_product_entity_text where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = '"
									+ tarjeton.getSku()
									+ "') AND attribute_id =165");
					if (!rsQuery.equals(null) || !rsQuery.equals("")) {
						instruccion
								.executeUpdate("update catalog_product_entity_text set value='"
										+ tarjeton.getAtributoTarjeton()
										+ "' where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = '"
										+ tarjeton.getSku()
										+ "') AND attribute_id =165");
					} else {
						instruccion
								.executeUpdate("insert into catalog_product_entity_text(entity_type_id,attribute_id, store_id, entity_id, value) values(4, 165, 0, (SELECT entity_id FROM catalog_product_entity WHERE sku = '"
										+ tarjeton.getSku()
										+ "'), '"
										+ tarjeton.getAtributoTarjeton() + "')");
					}
				}

				System.out.println("Inserto y actualizo tarjeton de productos");
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

	}

	public static void insertDestacadoWeb(List<DestacadosWeb> listScriptInserts) {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			instruccion.executeQuery("truncate table _destacados_web");
			for (DestacadosWeb destacadosWeb : listScriptInserts) {
				instruccion
						.executeUpdate("insert into _destacados_web(DMARCA, DITEM, DCATEGORIA, DFECHAI, DFECHAF, DFECHA_INGRESO, DDESTACADO) values("
								+ destacadosWeb.getDmarca()
								+ ","
								+ destacadosWeb.getDitem()
								+ ",'"
								+ destacadosWeb.getDcategoria()
								+ "','"
								+ destacadosWeb.getDfechai()
								+ "','"
								+ destacadosWeb.getDfechaf()
								+ "','"
								+ destacadosWeb.getDfecha_ingreso()
								+ "','"
								+ destacadosWeb.getDdestacado() + "')");
			}

			System.out
					.println("Inserto y actualizo en tabla temporal _destacados_web");
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

	public static void publicarPromocionesWeb(
			List<PromocionesWeb> listScriptInserts) {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();
			Date fechaActual = new Date();

			for (PromocionesWeb promocionesWeb : listScriptInserts) {
				ResultSet rsQuery = instruccion
						.executeQuery("select sku from catalog_product_entity where sku='"
								+ promocionesWeb.getImarca()
								+ "-"
								+ promocionesWeb.getIcod() + "'");
				//System.out.println("select sku from catalog_product_entity where sku='" + promocionesWeb.getImarca() + "-" + promocionesWeb.getIcod() + "'");

				if (rsQuery.next()) {
					ResultSet rsQueryCCP = instruccion
							.executeQuery("select category_id, product_id from catalog_category_product where product_id=(select entity_id from catalog_product_entity where sku='"
									+ promocionesWeb.getImarca()
									+ "-"
									+ promocionesWeb.getIcod()
									+ "') and category_id=149");

					if (rsQueryCCP.next()) {
						if (fechaActual.after(promocionesWeb.getIfecha1())
								&& fechaActual.before(promocionesWeb
										.getIfecha2())
								|| fechaActual.compareTo(promocionesWeb
										.getIfecha1()) == 0
								|| fechaActual.compareTo(promocionesWeb
										.getIfecha2()) == 0) {
							instruccion
									.executeUpdate("delete from catalog_category_product where category_id=149 and product_id=(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod() + "')");
							instruccion
									.executeUpdate("delete from catalog_category_product_index where category_id=149 and product_id=(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod() + "')");
							instruccion
									.executeUpdate("insert into catalog_category_product(category_id,product_id,position)values(149,(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod()
											+ "'),1)");
							instruccion
									.executeUpdate("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values(149,(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod()
											+ "'),1,1,1,4)");

						} else {
							instruccion
									.executeUpdate("delete from catalog_category_product where category_id=149 and product_id=(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod() + "')");
							instruccion
									.executeUpdate("delete from catalog_category_product_index where category_id=149 and product_id=(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod() + "')");
						}
					} else {
						if (fechaActual.after(promocionesWeb.getIfecha1())
								&& fechaActual.before(promocionesWeb
										.getIfecha2())
								|| fechaActual.compareTo(promocionesWeb
										.getIfecha1()) == 0
								|| fechaActual.compareTo(promocionesWeb
										.getIfecha2()) == 0) {
							instruccion
									.executeUpdate("insert into catalog_category_product(category_id,product_id,position)values(149,(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod() + "'),1)");
							instruccion
									.executeUpdate("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values(149,(select entity_id from catalog_product_entity where sku='"
											+ promocionesWeb.getImarca()
											+ "-"
											+ promocionesWeb.getIcod()
											+ "'),1,1,1,4)");
						}
					}

				}
			}

			System.out
					.println("termino actualizaciones de promociones del portal");
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

	public static void insertScripts(List<MaestroTarjeton> listScriptInserts) {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			// vacia la tabla temporal _maestro_tarjeton para volver a insertar
			// con la nueva informacion
			instruccion.executeQuery("truncate table _maestro_tarjeton");

			for (MaestroTarjeton string : listScriptInserts) {
				instruccion
						.executeUpdate("INSERT INTO _maestro_tarjeton(MARCA,ITEM,MODELO,VALOR_GARANTIA,LEGALES,PRECIO,NOMBRE_PRECIO_OFERTA,PRECIO_OFERTA,DESCRIPCION_PROMO1,FECHA_DESDE,FECHA_HASTA,PROMOCION) values("
								+ string.getMtCodigo()
								+ ","
								+ string.getMtItem()
								+ ",'"
								+ string.getMtModelo()
								+ "',"
								+ string.getMtValorGarantia()
								+ ",'"
								+ string.getMtLegales()
								+ "',"
								+ string.getMtPrecio()
								+ ",'"
								+ string.getMtNombreMarca()
								+ "',"
								+ string.getMtPrecioOferta()
								+ ",'"
								+ string.getMtDescripcionPromo1()
								+ "','"
								+ string.getMtFechaDesde()
								+ "','"
								+ string.getMtFechaHasta()
								+ "',"
								+ string.getMtPromocion() + ")");

			}

			System.out
					.println("Inserto y actualizo en tablas temporales de _maestro_tarjeton");
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

}

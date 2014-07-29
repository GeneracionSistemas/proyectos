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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Copy_4_of_ProcedimientoOracle {

	public static StringBuilder procesarORACLE(StringBuilder sentencia) {
		Connection conexion = null;
		StringBuilder resultado = new StringBuilder();
		try {

			// funcion para poner checks a la categoria padre de todos productos
			// checkRootCategory();

			Class.forName("oracle.jdbc.driver.OracleDriver");
			conexion = DriverManager.getConnection(
					"jdbc:oracle:thin:@//200.105.234.42:7975/sboddb.sukasa",
					"PORTAL_CLIENTE", "PCLIENTE");

			// Crear objeto Statement para realizar queries a la base de datos
			Statement instruccion = conexion.createStatement();
			resultado.append("PROCESOS EN TABLAS DE ORACLE");

			// Selecciona el kcodigo, kmarca,
			// kitem,kcantidad,kprecio,kprecio_afil de la tabla kardex de oracle
			// para actualizar el precio, precio sukasa, cantidad de los
			// productos de la pagina web

			/*
			String queryKardexOracle = "select CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('INSERT INTO _kardex(KCODIGO, KMARCA, KITEM, KPRECIO, KPRECIO_AFIL, KCANTIDAD, KACTUALIZACION, KNO_LOTE) values(',KCODIGO), ','), KMARCA), ','), KITEM), ','), TO_CHAR(KPRECIO, '99999.99')), ','), TO_CHAR(KPRECIO_AFIL, '99999.99')), ','), KCANTIDAD), ',0,0)') as kardexItems, kcodigo, kmarca, kitem, kprecio, kprecio_afil, kcantidad from kardex  inner join items i on kmarca=i.imarca and kitem =i.iitem where kactualizacion=0 order by kcodigo asc";
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
			*/
			
			
			// Selecciona productos de la tabla maestro_tarjeton de Sukasa

			// String queryMaestroTarjeton =
			// "select * from maestro_tarjeton where MTMARCA=3289 and MTITEM=312";
			 String queryMaestroTarjeton =
			 "select * from maestro_tarjeton where MTMARCA=788 and MTITEM=723";
			//String queryMaestroTarjeton = "select * from maestro_tarjeton";

			ResultSet rsQueryMaestroTarjeton = instruccion
					.executeQuery(queryMaestroTarjeton);

			List<MaestroTarjeton> listMaestroTarjeton = new ArrayList<MaestroTarjeton>();
			DateFormat dateFormatMT = new SimpleDateFormat("dd/mm/yyyy");
			Date dateNull = dateFormatMT.parse("6/6/1999");
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
				mtnombre_marca = mtnombre_marca.replaceAll("'", "&nbsp;");
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
					String fechaDesde = "1999-6-6";
					maestroTarjeton.setMtFechaDesde(fechaDesde);
					maestroTarjeton.setMtFechaDesde1(dateNull);
				} else {
					String fechaDesde = rsQueryMaestroTarjeton.getDate(
							"MTFECHA_DESDE").toString();
					maestroTarjeton.setMtFechaDesde(fechaDesde);
					maestroTarjeton.setMtFechaDesde1(rsQueryMaestroTarjeton
							.getDate("MTFECHA_DESDE"));
				}

				if (rsQueryMaestroTarjeton.getDate("MTFECHA_HASTA") == null) {
					String fechaHasta = "1999-6-6";
					maestroTarjeton.setMtFechaHasta(fechaHasta);
					maestroTarjeton.setMtFechaHasta1(dateNull);
				} else {
					String fechaHasta = rsQueryMaestroTarjeton.getDate(
							"MTFECHA_HASTA").toString();
					maestroTarjeton.setMtFechaHasta(fechaHasta);
					maestroTarjeton.setMtFechaHasta1(rsQueryMaestroTarjeton
							.getDate("MTFECHA_HASTA"));
				}

				maestroTarjeton.setMtPromocion(rsQueryMaestroTarjeton
						.getInt("MT_PROMOCION"));

				listMaestroTarjeton.add(maestroTarjeton);
			}

			// Recolecta informacion para calcular las cuotas mensuales de los
			// productos en el tarjeton del portal de las tablas
			// param_tarjeton_suk, promocion y fpmes_suk de sukasa

			String queryParamTarjeton = "select * from param_tarjeton_suk";
			ResultSet rsQueryParamTarjeton = instruccion
					.executeQuery(queryParamTarjeton);
			List<ParamTarjeton> listParamTarjeton = new ArrayList<ParamTarjeton>();

			while (rsQueryParamTarjeton.next()) {
				ParamTarjeton paramTarjeton = new ParamTarjeton();
				paramTarjeton.setPt_grupo(rsQueryParamTarjeton
						.getInt("PT_GRUPO"));
				paramTarjeton.setPt_nmeses(rsQueryParamTarjeton
						.getInt("PT_NMESES"));
				paramTarjeton.setPt_pvpi(rsQueryParamTarjeton
						.getDouble("PT_PVPI"));
				paramTarjeton.setPt_pvpf(rsQueryParamTarjeton
						.getDouble("PT_PVPF"));
				paramTarjeton.setPt_min_con_descuento(rsQueryParamTarjeton
						.getDouble("PT_MIN_CON_DESCUENTO"));
				listParamTarjeton.add(paramTarjeton);
			}

			String queryPromocion = "select * from promocion order by pcod, pmes asc";
			ResultSet rsQueryPromocion = instruccion
					.executeQuery(queryPromocion);
			List<Promocion> listPromocion = new ArrayList<Promocion>();

			while (rsQueryPromocion.next()) {
				Promocion promocion = new Promocion();
				promocion.setPcod(rsQueryPromocion.getInt("PCOD"));
				promocion.setPformapago(rsQueryPromocion.getInt("PFORMAPAGO"));
				promocion.setPtipo(rsQueryPromocion.getInt("PTIPO"));
				promocion.setPmes(rsQueryPromocion.getInt("PMES"));
				promocion.setPfactor(rsQueryPromocion.getDouble("PFACTOR"));
				listPromocion.add(promocion);
			}

			String queryFpmes = "select * from fpmes_suk order by mmes asc";
			ResultSet rsQueryFpmes = instruccion.executeQuery(queryFpmes);
			List<Fpmes> listFpmes = new ArrayList<Fpmes>();

			while (rsQueryFpmes.next()) {
				Fpmes fpmes = new Fpmes();
				fpmes.setMmes(rsQueryFpmes.getInt("MMES"));
				fpmes.setMdesc(rsQueryFpmes.getDouble("MDESC"));
				listFpmes.add(fpmes);
			}

			// funcion que actualiza el atributo tarjeton de los productos de la
			// pagina web
			if (listParamTarjeton.size() > 0 && listPromocion.size() > 0
					&& listFpmes.size() > 0 && listMaestroTarjeton.size() > 0) {
				actualizarTarjetonProductos(listParamTarjeton, listPromocion,
						listFpmes, listMaestroTarjeton);
			}
			// Creacion un objeto lista de productos destados web para
			// insertarlos en la tabla _destacados_web

			/*
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

			String queryKardexSukasa = "select * from kardex where kcantidad>0";

			ResultSet rsQueryKardexSukasa = instruccion
					.executeQuery(queryKardexSukasa);

			List<MaestroTarjeton> listProductosNoBloqueadosKardex = new ArrayList<MaestroTarjeton>();

			while (rsQueryKardexSukasa.next()) {
				MaestroTarjeton productoBloqueado = new MaestroTarjeton();
				productoBloqueado.setMtCodigo(rsQueryKardexSukasa
						.getInt("KMARCA"));
				productoBloqueado
						.setMtItem(rsQueryKardexSukasa.getInt("KITEM"));
				listProductosNoBloqueadosKardex.add(productoBloqueado);
			}

			if (listProductosNoBloqueadosKardex.size() > 0) {
				habilitarNoBloqueados(listProductosNoBloqueadosKardex);
				deshabilitarBloqueados(listProductosNoBloqueadosKardex);
			}

			// Selecciona los productos para ponerlos en las diferentes
			// categor’as de programas del portal

			String queryProgramasPortal = "SELECT CONCAT(CONCAT(IMARCA, '-'),ICOD) as SKU, DESCRIPCION FROM programa_portal";
			ResultSet rsQueryProgramasPortal = instruccion
					.executeQuery(queryProgramasPortal);
			List<Programa> lstProductosPrograma = new ArrayList<Programa>();
			while (rsQueryProgramasPortal.next()) {
				Programa productoPrograma = new Programa();
				productoPrograma
						.setSku(rsQueryProgramasPortal.getString("SKU"));
				productoPrograma.setDescripcion(rsQueryProgramasPortal
						.getString("DESCRIPCION"));
				lstProductosPrograma.add(productoPrograma);
			}

			if (lstProductosPrograma.size() > 0) {
				habilitar_programas_portal(lstProductosPrograma);
			}

			// Funcion para actualizar las urls duplicadas en el portal
			urlsDuplicadas();

			// Funcion para cambiar el estado de disponibilidad en inventario de
			// todos
			// los productos al estado EN EXISTENCIA
			stockAvailability();
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
		try {
			Class.forName("com.mysql.jdbc.Driver");
//			 conexion = DriverManager
//			 .getConnection(
//			 "jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
//			 "root", "");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
							"root", "yagehosting");
			Statement instruccion = conexion.createStatement();

			for (Kardex string : listScriptInserts) {
				System.out.println("SKU KARDEX:" + string.getKmarca() + "-"
						+ string.getKitem());
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

	public static void ocultarProductosCantidadCero() {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			ResultSet rsQuery = instruccion
					.executeQuery("SELECT csi.product_id FROM catalog_product_entity cpe, cataloginventory_stock_item csi WHERE cpe.entity_id = csi.product_id AND cpe.sku NOT LIKE 'receta%' AND cpe.sku NOT LIKE 'secre%' and cpe.sku not like '0.%' AND csi.qty =0");
			while (rsQuery.next()) {
				Statement instruccion1 = conexion.createStatement();
				instruccion1
						.executeUpdate("update catalog_product_entity_int set value=2 where entity_id="
								+ rsQuery.getInt("product_id")
								+ " and  attribute_id =84");
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

	public static void publicarProductosSinCantidadCero() {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();
			ResultSet rsQueryPublicar = instruccion
					.executeQuery("SELECT csi.product_id FROM catalog_product_entity cpe, cataloginventory_stock_item csi WHERE cpe.entity_id = csi.product_id AND cpe.sku NOT LIKE 'receta%' AND cpe.sku NOT LIKE 'secre%' and cpe.sku not like '0.%' AND csi.qty >0");

			while (rsQueryPublicar.next()) {
				Statement instruccion1 = conexion.createStatement();
				instruccion1
						.executeUpdate("update catalog_product_entity_int set value=1 where entity_id="
								+ rsQueryPublicar.getInt("product_id")
								+ " and  attribute_id =84");
			}

			System.out
					.println("Se publico productos con cantidad en stock > 0");
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
			List<Promocion> listPromocion, List<Fpmes> listFpmes,
			List<MaestroTarjeton> listMaestroTarjeton) {

		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
			// "root", "");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
							"root", "yagehosting");

			Statement instruccion2 = conexion.createStatement();
			List<Tarjeton> listTarjeton = new ArrayList<Tarjeton>();
			char comillas = '"';
			int grupo = 0;
			int codPromocionMT = 0;

			DecimalFormat df = new DecimalFormat("####.00");
			DecimalFormat df1 = new DecimalFormat("#,###,##0.000000");

			for (MaestroTarjeton maestro_tarjeton : listMaestroTarjeton) {
				Date fechaActual = new Date();
				DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
				String dfecha1 = maestro_tarjeton.getMtFechaDesde();
				Date dFecha1 = dateFormat.parse(dfecha1);
				String dfecha2 = maestro_tarjeton.getMtFechaHasta();
				Date dFecha2 = dateFormat.parse(dfecha2);
				String precioOferta = new String();
				boolean textoCuota = false;

				grupo = 0;
				Tarjeton productoTarjeton = new Tarjeton();
				List<DescuentoMesesPromocion> listDescuentoMesesPromocion = new ArrayList<DescuentoMesesPromocion>();
				List<Integer> numeroMeses = new ArrayList<Integer>();
				List<Integer> numeroMesesTarjeton = new ArrayList<Integer>();
				List<String> cuotasTarjetonPublicar = new ArrayList<String>();
				for (ParamTarjeton paramTarjeton : listParamTarjeton) {
					if (maestro_tarjeton.getMtPrecio() >= paramTarjeton
							.getPt_pvpi()
							&& maestro_tarjeton.getMtPrecio() <= paramTarjeton
									.getPt_pvpf()) {
						grupo = paramTarjeton.getPt_grupo();
						break;
					}
				}

				for (ParamTarjeton paramTarjetonMeses : listParamTarjeton) {
					if (paramTarjetonMeses.getPt_grupo() == grupo) {
						numeroMeses.add(paramTarjetonMeses.getPt_nmeses());
					}
				}

				Collections.sort(numeroMeses);

				productoTarjeton.setSku(maestro_tarjeton.getMtCodigo() + "-"
						+ maestro_tarjeton.getMtItem());

				// selecciona el codigo de la promocion para consultar el
				// porcentaje a utilizar en el calculo de las cuotitas
				codPromocionMT = maestro_tarjeton.getMtPromocion();

				if (codPromocionMT != 0) {
					for (Promocion promocionlista : listPromocion) {
						if (promocionlista.getPcod() == codPromocionMT) {
							for (int i = 0; i < numeroMeses.size(); i++) {
								if (promocionlista.getPmes() == numeroMeses
										.get(i)) {
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
					for (Fpmes fpmes : listFpmes) {
						for (Integer integer : numeroMeses) {

							if (fpmes.getMmes() == integer.intValue()) {
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
					for (ParamTarjeton paramTarjeton : listParamTarjeton) {
						for (DescuentoMesesPromocion descuentoMesesPromocion : listDescuentoMesesPromocion) {
							if (paramTarjeton.getPt_nmeses() == descuentoMesesPromocion
									.getMeses()
									&& paramTarjeton.getPt_grupo() == grupo) {

								if (descuentoMesesPromocion.getDescuento() == 0) {
									double valorCuota = maestro_tarjeton
											.getMtPrecio()
											/ descuentoMesesPromocion
													.getMeses();
									double valorMensual = valorCuota
											* descuentoMesesPromocion
													.getMeses();
									if (valorCuota >= paramTarjeton
											.getPt_min_con_descuento()) {
										numeroMesesTarjeton
												.add(descuentoMesesPromocion
														.getMeses());

										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas
												+ "cuotas"
												+ comillas
												+ "><span class="
												+ comillas
												+ "numCuotas"
												+ comillas
												+ ">"
												+ descuentoMesesPromocion
														.getMeses()
												+ "</span><span class="
												+ comillas
												+ "palCuotas"
												+ comillas
												+ "> cuotas de </span><span class="
												+ comillas
												+ "usdCuotas"
												+ comillas
												+ ">$ "
												+ df.format(valorCuota)
												+ "<sup style="
												+ comillas
												+ "font-size:18px;"
												+ comillas
												+ "></sup></span></div>";

										if (grupo == 1) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
										} else if (grupo == 2) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
											cuotasTarjetonPublicar
													.add(cuotasProductoTarjeton);
										} else if (grupo == 3) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
											cuotasTarjetonPublicar
													.add(cuotasProductoTarjeton);
										}
									}
								} else {
									double factorDescuento = descuentoMesesPromocion
											.getDescuento();
									double factor = (double) factorDescuento / 100;
									double cuota = maestro_tarjeton
											.getMtPrecio() * factor;
									String cuotas = df1.format(cuota);
									String[] parts = cuotas.split(",");
									String part1 = parts[0];
									String part2 = parts[1];
									String cuotaDecimales = part1.replace(".",
											"") + "," + part2.substring(0, 2);
									double cuotaTruncada = Double
											.parseDouble(cuotaDecimales
													.replace(",", "."));

									double valorMensual = maestro_tarjeton
											.getMtPrecio() - cuotaTruncada;
									double valorCuota = valorMensual
											/ descuentoMesesPromocion
													.getMeses();

									String valorCuotas = df1.format(valorCuota);
									String[] partsC = valorCuotas.split(",");
									String part1C = partsC[0];
									String part2C = partsC[1];

									String cuotaDecimalesC = part1C.replace(
											".", "")
											+ ","
											+ part2C.substring(0, 2);
									double cuotaTruncadaC = Double
											.parseDouble(cuotaDecimalesC
													.replace(",", "."));
									valorCuota = cuotaTruncadaC;

									valorMensual = descuentoMesesPromocion
											.getMeses() * valorCuota;
									String valorMensualC = df1
											.format(valorMensual);
									String[] partsMC = valorMensualC.split(",");
									String part1MC = partsMC[0];
									String part2MC = partsMC[1];

									String valorMensualSC = part1MC.replace(
											".", "")
											+ ","
											+ part2MC.substring(0, 2);
									double valorMensualTruncado = Double
											.parseDouble(valorMensualSC
													.replace(",", "."));
									valorMensual = valorMensualTruncado;

									if (valorCuota >= paramTarjeton
											.getPt_min_con_descuento()) {
										numeroMesesTarjeton
												.add(descuentoMesesPromocion
														.getMeses());
										cuotaResaltarProductoTarjeton = "<div class="
												+ comillas
												+ "cuotas"
												+ comillas
												+ "><span class="
												+ comillas
												+ "numCuotas"
												+ comillas
												+ ">"
												+ descuentoMesesPromocion
														.getMeses()
												+ "</span><span class="
												+ comillas
												+ "palCuotas"
												+ comillas
												+ "> cuotas de </span><span class="
												+ comillas
												+ "usdCuotas"
												+ comillas
												+ ">$ "
												+ df.format(valorCuota)
												+ "<sup style="
												+ comillas
												+ "font-size:18px;"
												+ comillas
												+ "></sup></span></div>";

										if (grupo == 1) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
										} else if (grupo == 2) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
											cuotasTarjetonPublicar
													.add(cuotasProductoTarjeton);
										} else if (grupo == 3) {
											cuotasProductoTarjeton = "<td><p>"
													+ descuentoMesesPromocion
															.getMeses()
													+ "  MESES</p><p style="
													+ comillas
													+ "font-size:14px"
													+ comillas + "> X US $ "
													+ df.format(valorCuota)
													+ " </p><p style="
													+ comillas + "18px"
													+ comillas + ">$ "
													+ df.format(valorMensual)
													+ "</p></td>";
											cuotasTarjetonPublicar
													.add(cuotasProductoTarjeton);
										}
									}
								}
							}
						}
					}

					if (cuotaResaltarProductoTarjeton.length() > 0) {
						int k = 0;
						cuotasProductoTarjeton = "";
						for (String string : cuotasTarjetonPublicar) {
							if (k < numeroMesesTarjeton.size() - 1) {
								cuotasProductoTarjeton = cuotasProductoTarjeton
										+ string.toString();
								textoCuota = true;
								k++;
							}
						}
					}

					String tarjeton = new String();
					String cuotaResaltar = new String();
					tarjeton = "<div class=" + comillas + "stdCuotas"
							+ comillas + "><div class=" + comillas + "formUno"
							+ comillas + "><span class=" + comillas + "code"
							+ comillas + "> CODIGO: "
							+ maestro_tarjeton.getMtCodigo() + "-"
							+ maestro_tarjeton.getMtItem()
							+ "</span><span class=" + comillas + "code"
							+ comillas + "> MODELO: "
							+ maestro_tarjeton.getMtModelo() + "</span><p id="
							+ comillas + "PVP" + comillas + ">PVP: USD$ "
							+ maestro_tarjeton.getMtPrecio() + "</p>";

					cuotaResaltar = cuotaResaltarProductoTarjeton;

					tarjeton = tarjeton + cuotaResaltar;

					String fechasPromocion = new String();
					if (!maestro_tarjeton.getMtFechaDesde().toString()
							.equals("1999-6-6")) {
						fechasPromocion = "<p class=" + comillas + "code"
								+ comillas + ">Del: "
								+ maestro_tarjeton.getMtFechaDesde() + " al "
								+ maestro_tarjeton.getMtFechaHasta()
								+ "</p><br/>";
					} else {
						fechasPromocion = "";
					}

					String descripcionPromo = new String();
					descripcionPromo = maestro_tarjeton
							.getMtDescripcionPromo1();
					descripcionPromo = descripcionPromo.replaceAll("363",
							"&oacute;");

					String cuotas = new String();

					if (textoCuota == true) {
						cuotas = "Cuotas:";
					}

					if (fechaActual.after(dFecha1)
							&& fechaActual.before(dFecha2)
							|| fechaActual.compareTo(dFecha1) == 0
							|| fechaActual.compareTo(dFecha2) == 0) {
						precioOferta = "<p class=" + comillas + "code"
								+ comillas + ">"
								+ maestro_tarjeton.getMtNombreMarca() + " $ "
								+ maestro_tarjeton.getMtPrecioOferta() + "</p>";
					} else {
						precioOferta = "<p class=" + comillas + "code"
								+ comillas + ">"
								+ maestro_tarjeton.getMtNombreMarca() + "</p>";
					}

					tarjeton = tarjeton + "</div>" + precioOferta + "<p class="
							+ comillas + "code" + comillas + ">"
							+ descripcionPromo + "</p>" + fechasPromocion
							+ "<p class=" + comillas + "code" + comillas
							+ "><br/><p class=" + comillas + "code" + comillas
							+ ">" + cuotas + "</p><table class=" + comillas
							+ "Tabcuotas" + comillas + "><tr>";

					tarjeton = tarjeton + cuotasProductoTarjeton;

					if (maestro_tarjeton.getMtValorGarantia() > 0) {
						tarjeton = tarjeton
								+ "</tr></table><p class="
								+ comillas
								+ "code"
								+ comillas
								+ ">Extensi&oacute;n de Garant&iacute;a Original: $"
								+ maestro_tarjeton.getMtValorGarantia()
								+ "</p><p class=" + comillas + "legales"
								+ comillas + ">"
								+ maestro_tarjeton.getMtLegales()
								+ "</p></div>";
					} else {
						tarjeton = tarjeton
								+ "</tr></table><p class="
								+ comillas
								+ "code"
								+ comillas
								+ ">Extensi&oacute;n de Garant&iacute;a Original: No Aplica</p><p class="
								+ comillas + "legales" + comillas + ">"
								+ maestro_tarjeton.getMtLegales()
								+ "</p></div>";

					}

					productoTarjeton.setAtributoTarjeton(tarjeton);

					listTarjeton.add(productoTarjeton);

				} else {
					String tarjeton = new String();
					String cuotaResaltar = new String();
					tarjeton = "<div class=" + comillas + "stdCuotas"
							+ comillas + "><div class=" + comillas + "formUno"
							+ comillas + "><span class=" + comillas + "code"
							+ comillas + "> CODIGO: "
							+ maestro_tarjeton.getMtCodigo() + "-"
							+ maestro_tarjeton.getMtItem()
							+ "</span><span class=" + comillas + "code"
							+ comillas + "> MODELO: "
							+ maestro_tarjeton.getMtModelo() + "</span><p id="
							+ comillas + "PVP" + comillas + ">PVP: USD$ "
							+ maestro_tarjeton.getMtPrecio() + "</p>";

					cuotaResaltar = cuotaResaltarProductoTarjeton;

					tarjeton = tarjeton + cuotaResaltar;

					String fechasPromocion = new String();
					if (!maestro_tarjeton.getMtFechaDesde().toString()
							.equals("1999-6-6")) {
						fechasPromocion = "<p class=" + comillas + "code"
								+ comillas + ">Del: "
								+ maestro_tarjeton.getMtFechaDesde() + " al "
								+ maestro_tarjeton.getMtFechaHasta()
								+ "</p><br/>";
					} else {
						fechasPromocion = "";
					}

					String descripcionPromo = new String();
					descripcionPromo = maestro_tarjeton
							.getMtDescripcionPromo1();
					descripcionPromo = descripcionPromo.replaceAll("363",
							"&oacute;");

					String cuotas = new String();
					if (numeroMeses.size() > 0) {
						cuotas = "Cuotas:";
					}

					tarjeton = tarjeton + "</div>" + precioOferta + "<p class="
							+ comillas + "code" + comillas + ">"
							+ descripcionPromo + "</p>" + fechasPromocion
							+ "<p class=" + comillas + "code" + comillas
							+ "><br/><p class=" + comillas + "code" + comillas
							+ ">" + cuotas + "</p><table class=" + comillas
							+ "Tabcuotas" + comillas + "><tr>";

					tarjeton = tarjeton + cuotasProductoTarjeton;

					if (maestro_tarjeton.getMtValorGarantia() > 0) {
						tarjeton = tarjeton
								+ "</tr></table><p class="
								+ comillas
								+ "code"
								+ comillas
								+ ">Extensi&oacute;n de Garant&iacute;a Original: $"
								+ maestro_tarjeton.getMtValorGarantia()
								+ "</p><p class=" + comillas + "legales"
								+ comillas + ">"
								+ maestro_tarjeton.getMtLegales()
								+ "</p></div>";
					} else {
						tarjeton = tarjeton
								+ "</tr></table><p class="
								+ comillas
								+ "code"
								+ comillas
								+ ">Extensi&oacute;n de Garant&iacute;a Original: No Aplica</p><p class="
								+ comillas + "legales" + comillas + ">"
								+ maestro_tarjeton.getMtLegales()
								+ "</p></div>";

					}

					System.out.println("CODIGO: "
							+ maestro_tarjeton.getMtCodigo() + "-"
							+ maestro_tarjeton.getMtItem());
					productoTarjeton.setAtributoTarjeton(tarjeton);

					listTarjeton.add(productoTarjeton);
				}
			}

			if (listTarjeton.size() > 0) {
				for (Tarjeton tarjeton : listTarjeton) {
					ResultSet rsQuery = instruccion2
							.executeQuery("select * from catalog_product_entity_text where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = '"
									+ tarjeton.getSku()
									+ "') AND attribute_id =165");
					System.out.println(tarjeton.getSku());
					if (!rsQuery.equals(null) || !rsQuery.equals("")) {
						instruccion2
								.executeUpdate("update catalog_product_entity_text set value='"
										+ tarjeton.getAtributoTarjeton()
										+ "' where entity_id=(SELECT entity_id FROM catalog_product_entity WHERE sku = '"
										+ tarjeton.getSku()
										+ "') AND attribute_id =165");

					} else {
						instruccion2
								.executeUpdate("insert into catalog_product_entity_text(entity_type_id,attribute_id, store_id, entity_id, value) values(4, 165, 0, (SELECT entity_id FROM catalog_product_entity WHERE sku = '"
										+ tarjeton.getSku()
										+ "'), '"
										+ tarjeton.getAtributoTarjeton() + "')");
					}
				}
			}
			System.out.println("Inserto y actualizo tarjeton de productos");
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
											+ promocionesWeb.getIcod()
											+ "'),1)");
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

	public static void deshabilitarBloqueados(
			List<MaestroTarjeton> listProductosBloqueados) {
		System.out.println("entro deshabilitar bloqueados");
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			Statement instruccion = conexion.createStatement();

			String codigosBloqueados = new String();
			List<String> listCodigosBloqueados = new ArrayList<String>();

			for (MaestroTarjeton maestroTarjeton : listProductosBloqueados) {
				listCodigosBloqueados.add("'" + maestroTarjeton.getMtCodigo()
						+ "-" + maestroTarjeton.getMtItem() + "'");
			}

			int i = 0;
			for (String string : listCodigosBloqueados) {
				i++;
				if (i != listCodigosBloqueados.size()) {
					codigosBloqueados = codigosBloqueados + string.toString()
							+ ",";
				} else {
					codigosBloqueados = codigosBloqueados + string.toString();
				}

			}

			ResultSet rsQuery = instruccion
					.executeQuery("SELECT entity_id FROM catalog_product_entity WHERE sku NOT IN ("
							+ codigosBloqueados
							+ ") and sku not like 'receta%' and sku not like 'secre%' and sku not like '0.%' and sku not like 'clcc%' and sku not like 'catg%'");
			while (rsQuery.next()) {
				Statement instruccion1 = conexion.createStatement();
				instruccion1
						.executeUpdate("update catalog_product_entity_int set value=2 where entity_id="
								+ rsQuery.getInt("entity_id")
								+ " and  attribute_id =84");
			}

			System.out
					.println("Se oculto productos bloqueados en el portal que no estan en la tabla kardex de Sukasa");
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

	public static void habilitarNoBloqueados(
			List<MaestroTarjeton> listProductosNoBloqueados) {
		System.out.println("entro habilitar no bloqueados");
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			Statement instruccion = conexion.createStatement();

			String codigosNoBloqueados = new String();
			List<String> listCodigosNoBloqueados = new ArrayList<String>();

			for (MaestroTarjeton maestroTarjeton : listProductosNoBloqueados) {
				listCodigosNoBloqueados.add("'" + maestroTarjeton.getMtCodigo()
						+ "-" + maestroTarjeton.getMtItem() + "'");
			}

			int i = 0;
			for (String string : listCodigosNoBloqueados) {
				i++;
				if (i != listCodigosNoBloqueados.size()) {
					codigosNoBloqueados = codigosNoBloqueados
							+ string.toString() + ",";
				} else {
					codigosNoBloqueados = codigosNoBloqueados
							+ string.toString();
				}

			}

			ResultSet rsQuery = instruccion
					.executeQuery("SELECT entity_id FROM catalog_product_entity WHERE sku IN ("
							+ codigosNoBloqueados
							+ ") and sku not like 'receta%' and sku not like 'secre%' and sku not like '0.%' and sku not like 'clcc%' and sku not like 'catg%'");
			while (rsQuery.next()) {
				Statement instruccion1 = conexion.createStatement();
				instruccion1
						.executeUpdate("update catalog_product_entity_int set value=1 where entity_id="
								+ rsQuery.getInt("entity_id")
								+ " and  attribute_id =84");
			}

			System.out
					.println("Se publico productos no bloqueados en el portal con kardex>0");
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

	// Funcion para poner checks a la categoria padres de todos los productos
	public static void checkRootCategory() {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			String queryCodigos = "SELECT product_id FROM catalog_category_product WHERE category_id !=2 GROUP BY product_id";
			// String queryCodigos =
			// "SELECT product_id FROM catalog_category_product WHERE category_id !=2 and product_id=7813";
			ResultSet rsQueryCodigos = instruccion.executeQuery(queryCodigos);

			List<PromocionesWeb> listCodigos = new ArrayList<PromocionesWeb>();

			while (rsQueryCodigos.next()) {
				PromocionesWeb codigosCheck = new PromocionesWeb();
				codigosCheck.setIcod(rsQueryCodigos.getInt("product_id"));
				listCodigos.add(codigosCheck);
			}

			for (PromocionesWeb promocionesWeb : listCodigos) {

				System.out
						.println("delete from catalog_category_product where category_id=2 and product_id="
								+ promocionesWeb.getIcod() + "");
				System.out
						.println("delete from catalog_category_product_index where category_id=2 and product_id="
								+ promocionesWeb.getIcod() + "");
				System.out
						.println("insert into catalog_category_product(category_id,product_id,position)values(2,"
								+ promocionesWeb.getIcod() + ",1)");
				System.out
						.println("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values(2,"
								+ promocionesWeb.getIcod() + ",1,1,1,4)");

				instruccion
						.executeUpdate("delete from catalog_category_product where category_id=2 and product_id="
								+ promocionesWeb.getIcod() + "");
				instruccion
						.executeUpdate("delete from catalog_category_product_index where category_id=2 and product_id="
								+ promocionesWeb.getIcod() + "");
				instruccion
						.executeUpdate("insert into catalog_category_product(category_id,product_id,position)values(2,"
								+ promocionesWeb.getIcod() + ",1)");
				instruccion
						.executeUpdate("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values(2,"
								+ promocionesWeb.getIcod() + ",1,1,1,4)");
			}

			System.out
					.println("termino actualizaciones de checks categorias productos");
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

	/*
	 * Funcion que selecciona los productos para ponerlos en las diferentes
	 * categor’as de programas del portal
	 */
	public static void habilitar_programas_portal(
			List<Programa> productosPrograma) {
		Connection conexion = null;

		// pre-produccion
		// int id_categoria_tendencia = 540;

		// produccion
		int id_categoria_tendencia = 719;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");

			Statement instruccion = conexion.createStatement();
			List<Integer> list_entity_id_codigos = new ArrayList<Integer>();

			for (Programa programa : productosPrograma) {
				Integer id_producto = new Integer(0);
				int id_categoria_magento_programa = 0;
				boolean existe_tendencia = false;
				boolean existe_tendencia_programa = false;

				ResultSet rsQueryCodigos = instruccion
						.executeQuery("SELECT entity_id from catalog_product_entity where sku='"
								+ programa.getSku() + "'");
				while (rsQueryCodigos.next()) {
					id_producto = rsQueryCodigos.getInt("entity_id");
					list_entity_id_codigos.add(id_producto);
				}

				if (programa.getDescripcion().equals("WONDERS SEAS")) {
					id_categoria_magento_programa = 720;
				} else if (programa.getDescripcion().equals("LEATHER TOUCH")) {
					id_categoria_magento_programa = 2430;
				} else if (programa.getDescripcion().equals("DAISY DAYS")) {
					id_categoria_magento_programa = 2431;
				} else if (programa.getDescripcion().equals("MUNDIAL 2014")) {
					id_categoria_magento_programa = 2664;
				}

				if (id_producto != 0 && id_categoria_magento_programa != 0) {
					ResultSet rsQueryTendencias1 = instruccion
							.executeQuery("SELECT * FROM catalog_category_product WHERE category_id="
									+ id_categoria_tendencia
									+ " AND product_id=" + id_producto + "");
					while (rsQueryTendencias1.next()) {
						existe_tendencia = true;
						break;
					}

					if (!existe_tendencia) {
						// INSERTA CATEGORIA PADRE TENDENCIAS
						instruccion
								.executeUpdate("delete from catalog_category_product where category_id="
										+ id_categoria_tendencia
										+ " and product_id=" + id_producto + "");
						instruccion
								.executeUpdate("delete from catalog_category_product_index where category_id="
										+ id_categoria_tendencia
										+ " and product_id=" + id_producto + "");
						instruccion
								.executeUpdate("insert into catalog_category_product(category_id,product_id,position)values("
										+ id_categoria_tendencia
										+ ","
										+ id_producto + ",1)");
						instruccion
								.executeUpdate("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values("
										+ id_categoria_tendencia
										+ ","
										+ id_producto + ",1,1,1,4)");
					}

					ResultSet rsQueryTendencias2 = instruccion
							.executeQuery("SELECT * FROM catalog_category_product WHERE category_id="
									+ id_categoria_magento_programa
									+ " AND product_id=" + id_producto + "");
					while (rsQueryTendencias2.next()) {
						existe_tendencia_programa = true;
						break;
					}
					if (!existe_tendencia_programa) {
						// INSERTA CATEGORIAS SEALIFE,ETC DE TENDENCIAS
						instruccion
								.executeUpdate("delete from catalog_category_product where category_id="
										+ id_categoria_magento_programa
										+ " and product_id=" + id_producto + "");
						instruccion
								.executeUpdate("delete from catalog_category_product_index where category_id="
										+ id_categoria_magento_programa
										+ " and product_id=" + id_producto + "");
						instruccion
								.executeUpdate("insert into catalog_category_product(category_id,product_id,position)values("
										+ id_categoria_magento_programa
										+ ","
										+ id_producto + ",1)");
						instruccion
								.executeUpdate("insert into catalog_category_product_index(category_id,product_id,position,is_parent,store_id,visibility)values("
										+ id_categoria_magento_programa
										+ ","
										+ id_producto + ",1,1,1,4)");
					}
				}
			}
			System.out
					.println("termino actualizaciones de productos programa portal");
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

	// Funcion para cambiar el estado de disponibilidad en inventario de todos
	// los productos al estado EN EXISTENCIA
	public static void stockAvailability() {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			String queryUpdate = "update cataloginventory_stock_status set stock_status=1 where qty>0 and stock_status=0";
			instruccion.executeUpdate(queryUpdate);

			String queryUpdate1 = "update cataloginventory_stock_item set is_in_stock=1 where qty>0 and is_in_stock=0";
			instruccion.executeUpdate(queryUpdate1);

			System.out
					.println("termino actualizaciones de estados EN EXISTENCIA de productos");
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

	// Funcion para cambiar actualizar las urls duplicadas en el portal
	public static void urlsDuplicadas() {
		Connection conexion = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			// conexion = DriverManager
			// .getConnection(
			// "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false",
			// "root", "yagehosting");
			Statement instruccion = conexion.createStatement();
			Statement instruccion1 = conexion.createStatement();

			String querySelect = "SELECT value FROM catalog_product_entity_varchar WHERE attribute_id =86 and value is not NULL and value!='' GROUP BY value HAVING COUNT( * ) >1";
			ResultSet rsQuerySelect = instruccion.executeQuery(querySelect);
			List<String> listUrls = new ArrayList<String>();

			while (rsQuerySelect.next()) {
				if (!rsQuerySelect.getString("value").equals("")) {
					listUrls.add("'" + rsQuerySelect.getString("value") + "'");
				}
			}

			String stringConsulta = "";
			int i = 0;
			for (String string : listUrls) {
				if (i != listUrls.size()) {
					stringConsulta = stringConsulta + string.toString() + ",";
				} else {
					stringConsulta = stringConsulta + string.toString();
				}
				i++;
			}

			rsQuerySelect.close();

			String consultaProductos = stringConsulta.substring(0,
					stringConsulta.length() - 1) + "";

			String querySelect1 = "SELECT cpev.entity_id, cpev.value, cpe.sku FROM catalog_product_entity_varchar cpev  JOIN  catalog_product_entity cpe ON cpe.entity_id =  cpev .entity_id  WHERE `attribute_id`=86  AND cpev.entity_type_id = 4 AND  value IN ("
					+ consultaProductos + ")";

			ResultSet rsQuerySelect1 = instruccion.executeQuery(querySelect1);

			if (!rsQuerySelect1.equals(null) || !rsQuerySelect1.equals("")) {
				String codigoProd = new String();
				String nuevaUrl = new String();
				String[] codigoProd1 = new String[1];

				while (rsQuerySelect1.next()) {
					if (rsQuerySelect1.getString("sku") != null) {
						codigoProd = rsQuerySelect1.getString("sku");
					}

					if (!codigoProd.equals(null) && codigoProd != null) {
						codigoProd1 = codigoProd.split("-");
						if (codigoProd1.length >= 2) {
							if (!codigoProd1[0].equals("secre")
									&& !codigoProd1[0].equals("receta")) {
								nuevaUrl = "'"
										+ rsQuerySelect1.getString("value")
										+ "-" + codigoProd1[1] + "'";

								String queryUpdate = "update catalog_product_entity_varchar set value="
										+ nuevaUrl
										+ " where attribute_id =86 and entity_id="
										+ rsQuerySelect1.getInt("entity_id")
										+ " ";
								instruccion1.executeUpdate(queryUpdate);
							}
						}
					}
				}
			}

			System.out
					.println("termino actualizaciones de urls duplicadas de productos");
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

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

public class ProcedimientoOracle1 {

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
			Statement instruccion1 = conexion.createStatement();
			resultado.append("PROCESOS EN TABLAS DE ORACLE");

			String queryProductosIngresados = "select IMARCA, IITEM from items";
			ResultSet rsProductosIngresados = instruccion.executeQuery(queryProductosIngresados);
			List<String> itemsMarcaCodigo = new ArrayList<String>();
			while(rsProductosIngresados.next()){
				itemsMarcaCodigo.add("'"+rsProductosIngresados.getString("IMARCA")+"-"+rsProductosIngresados.getString("IITEM")+"'");
			}
			
			String stringConsulta = "";
			int i = 0;
			for (String string : itemsMarcaCodigo) {
				if(i!=itemsMarcaCodigo.size()){
					stringConsulta = stringConsulta + string.toString()+",";
				}else{
					stringConsulta = stringConsulta + string.toString();
				}
			}
			
			
			
			List<Item> listaItemsProductos = llenarTablaItems(stringConsulta);
			
			if(listaItemsProductos!=null && listaItemsProductos.size()>0){
				for (Item item : listaItemsProductos) {
//					ResultSet rsQueryItems = instruccion.executeQuery("select IMARCA, IITEM from items where IMARCA="+item.getMarca()+" AND IITEM="+item.getItem()+"");
//					if (rsQueryItems.equals(null) || rsQueryItems.equals("")) {
//						instruccion1.executeUpdate("INSERT INTO items (IMARCA, IITEM) VALUES ("
//								+ rsQueryItems.getString("marca") + ","
//								+ rsQueryItems.getString("item") + ")");
//						while(rsQueryItems.next()){
						System.out.println("INSERT INTO items (IMARCA, IITEM) VALUES ("
							+ item.getMarca() + ","
							+ item.getItem() + ")");
						instruccion1.executeUpdate("INSERT INTO items (IMARCA, IITEM) VALUES ("
									+ item.getMarca() + ","
									+ item.getItem() + ")");
						
//						}
//						
//					}
				}
				System.out.println("Actualizo e inserto productos en tabla items de sukasa");
				
			}
			
			// recupera los codigos de los items de los productos de la pagina
			// web para insertarlos en las tablas de oracle de sukasa
//			List<String> listItemsInsertarOracle = llenarDatosTablaItems();
//			
//			
//
//			if (listItemsInsertarOracle != null
//					&& listItemsInsertarOracle.size() > 0) {
//				String queryTruncate = "truncate table items";
//				instruccion.executeQuery(queryTruncate);
//				for (String string : listItemsInsertarOracle) {
//					// System.out.println(string.toString());
//					instruccion.executeUpdate(string.toString());
//				}
//				System.out
//						.println("Actualizo e inserto productos en tabla items de sukasa");
//			}

			
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

	public static List<String> llenarDatosTablaItems() {

		Connection conexion = null;
		List<String> listInsertsItemsOracle = new ArrayList<String>();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
//			conexion = DriverManager .getConnection(
//			  "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false"
//			  , "root", "yagehosting");
			 
			conexion = DriverManager
					.getConnection(
							"jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
							"root", "");
			Statement instruccion = conexion.createStatement();

			String queryItems = "SELECT cpe.sku, SUBSTRING_INDEX(cpe.sku,'-',1) AS marca, SUBSTRING_INDEX(cpe.sku,'-',-1) AS item,cpet.value FROM catalog_product_entity cpe, catalog_product_entity_text cpet where cpe.entity_id=cpet.entity_id and cpet.attribute_id=62 and cpe.sku not like 'receta%' and cpe.sku not like 'bano%' and cpe.sku not like 'secre%' and cpe.sku not like 'clcc%' and cpe.sku not like 'catg%'";

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

	public static List<Item> llenarTablaItems(String consulta) {

		Connection conexion = null;
		List<Item> itemsMarcaCodigo = new ArrayList<Item>();
		String consultaProductos = consulta.substring(0,
				consulta.length() - 1) + "";
		try {
			Class.forName("com.mysql.jdbc.Driver");
//			conexion = DriverManager .getConnection(
//					  "jdbc:mysql://74.50.52.51:3306/magento?jdbcCompliantTruncation=false"
//					  , "root", "yagehosting");
					 
					conexion = DriverManager
							.getConnection("jdbc:mysql://localhost/magento?jdbcCompliantTruncation=false",
								"root", "");
			
			Statement instruccion = conexion.createStatement();

			String queryItems = "SELECT cpe.sku, SUBSTRING_INDEX(cpe.sku,'-',1) AS marca, SUBSTRING_INDEX(cpe.sku,'-',-1) AS item,cpet.value FROM catalog_product_entity cpe, catalog_product_entity_text cpet where cpe.entity_id=cpet.entity_id and cpet.attribute_id=62 and cpe.sku not like 'receta%' and cpe.sku not like 'bano%' and cpe.sku not like 'secre%' and cpe.sku not like '0.%' and cpe.sku not like 'clcc%' and cpe.sku not like 'catg%' and cpe.sku not in("+consultaProductos+")";

			ResultSet rsQueryItems = instruccion.executeQuery(queryItems);
			
			while (rsQueryItems.next()) {
				Item itemProducto = new Item();
				itemProducto.setMarca(rsQueryItems.getString("marca"));
				itemProducto.setItem(rsQueryItems.getString("item"));
				itemsMarcaCodigo.add(itemProducto);
			}
		} catch (SQLException e) {
			System.out.println("Error de MySQL: " + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("Error Inesperado: " + e.getMessage());
		} finally {
			if (conexion != null) {
				try {
					conexion.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return itemsMarcaCodigo;
	}

}
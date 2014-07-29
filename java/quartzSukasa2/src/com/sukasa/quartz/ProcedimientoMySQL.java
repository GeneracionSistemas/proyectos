package com.sukasa.quartz;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ProcedimientoMySQL {

	public static StringBuilder procesarMYSQL(StringBuilder sentencia) {
		 Connection conexion = null;
		 StringBuilder resultado=new StringBuilder();
	        try {
	        	System.out.println("voy a procesar la sentencia "+sentencia+" con MYSQL");
	        	Class.forName("com.mysql.jdbc.Driver");
	        	//conexion = DriverManager.getConnection("jdbc:mysql://192.168.0.97:3306/fernando", "root", "mysql");
	        	conexion = DriverManager.getConnection("jdbc:mysql://74.50.52.51:3306/magento", "root", "yagehosting");
	        	//Crear objeto Statement para realizar queries a la base de datos
	        	Statement instruccion = conexion.createStatement();
	        	//Un objeto ResultSet, almacena los datos de resultados de una consulta
	        	//ResultSet resultadoQuery = instruccion.executeQuery("SELECT cod , nombre FROM datos");
	        	resultado.append("QUERYS QUE SE VAN GENERANDO EN MYSQL");
	          System.out.println(conexion);
	        } catch (SQLException e) {
	          System.out.println("Error de MySQL: " + e.getMessage());
	        } catch (Exception e) {
	          System.out.println("Error inesperado: " + e.getMessage());
	        } finally{
	        	if(conexion!=null){
	        		try {
						conexion.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
	        	}
	        }
	        return resultado;
	}
}

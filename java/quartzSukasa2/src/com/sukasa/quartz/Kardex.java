/**
 * 
 */
package com.sukasa.quartz;

/**
 * @author panchete
 *
 */
public class Kardex {
	
	int kcodigo;
	int kmarca;
	int kitem;
	int cantidad;
	double precio;
	double precioSukasa;
	String queryInsertar;
	
	public int getKcodigo() {
		return kcodigo;
	}
	public void setKcodigo(int kcodigo) {
		this.kcodigo = kcodigo;
	}
	public int getKmarca() {
		return kmarca;
	}
	public void setKmarca(int kmarca) {
		this.kmarca = kmarca;
	}
	public int getKitem() {
		return kitem;
	}
	public void setKitem(int kitem) {
		this.kitem = kitem;
	}
	public int getCantidad() {
		return cantidad;
	}
	public void setCantidad(int cantidad) {
		this.cantidad = cantidad;
	}
	public double getPrecio() {
		return precio;
	}
	public void setPrecio(double precio) {
		this.precio = precio;
	}
	public double getPrecioSukasa() {
		return precioSukasa;
	}
	public void setPrecioSukasa(double precioSukasa) {
		this.precioSukasa = precioSukasa;
	}
	public String getQueryInsertar() {
		return queryInsertar;
	}
	public void setQueryInsertar(String queryInsertar) {
		this.queryInsertar = queryInsertar;
	}
	
	

}

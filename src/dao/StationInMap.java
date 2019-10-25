package dao;

import java.util.List;

import generateTopology.Main;

public class StationInMap {
	public String poi_ID,name,roadlink_ID,in_out;
	public LonLat lonLat;
	public StationInMap() {
		// TODO Auto-generated constructor stub
	}
	public StationInMap(String poi_ID,String name) {
		// TODO Auto-generated constructor stub
		this.poi_ID=poi_ID;
		this.name=name;
		this.in_out=null;
	}
	@Override
	public String toString() {
		StringBuilder str=new StringBuilder(poi_ID);
		str.append(",").append(Main.fullWidth2halfWidth(name)).append(",").append(lonLat.toString()).append(",").append(in_out);
		return str.toString();
	}
	public static String listAddSeparator(List<String> list){
		StringBuilder str=new StringBuilder();
		int length=list.size();
		if(length > 0){
		for(int i=0;i<length-1;i++){
			str.append(list.get(i)).append("|");
		}
		str.append(list.get(length-1));
		}
		return str.toString();
	}
}

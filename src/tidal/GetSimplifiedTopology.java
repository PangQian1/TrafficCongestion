package tidal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

import dao.LonLat;
import dao.RoadLink;
import dao.StationInMap;
import util.Config;

/**
 * 与generateTopology包里的Main.java几近一致
 * @author PQ
 *
 */

public class GetSimplifiedTopology {
	public static void main(String[] args) {
		HashMap<String, RoadLink> id_Roadlink = GetSimplifiedTopology.getTopology_ID_RoadLink_RoadName_Station(
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/Rbj.MID",
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/Rbj.MIF",
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/other/PNamebeijing.mid",
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/index/POIbeijing.mid",
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/road/R_LNamebeijing.mid",
				"E:/G-1149/trafficCongestion/北京地图数据/beijing/road/R_Namebeijing.mid");
		try {
			//BufferedWriter writer=new BufferedWriter(new FileWriter("I:/programData/trafficCongestion/bjTopology.csv"));//得到高速公路的网络拓扑图
			BufferedWriter writer=new BufferedWriter(new FileWriter("E:/G-1149/trafficCongestion/网格化/LonLat_2.csv"));//得到link的经纬度序列
			for(Iterator<RoadLink> iterator=id_Roadlink.values().iterator();iterator.hasNext();){
				RoadLink loop_roadlink=iterator.next();
				//writer.write(loop_roadlink.toStringForTidal()+"\r\n");
				writer.write(loop_roadlink.toStringForLonLat()+"\r\n");
			}
			writer.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static HashMap<String, RoadLink> getTopology_ID_RoadLink(String mid_file_path, String mif_file_path) {
		try {
			BufferedReader file_mid = new BufferedReader(new FileReader(mid_file_path));
			BufferedReader file_mif = new BufferedReader(new FileReader(mif_file_path));
			int line = 0, mif_kuai = 0;
			List<String> lonLat_array = new ArrayList<String>();
			List<Integer> highway_lines = new ArrayList<Integer>(Config.HIGHWAY_COUNT);//按序记录了mid文件中的所有行
			List<String> highway_roadlink_id = new ArrayList<String>(Config.HIGHWAY_COUNT);//按序记录了mid文件中所有linkID
			HashMap<String, RoadLink> id_RoadLink = new HashMap<String, RoadLink>();//linkID,roadlink
			HashMap<String, List<String>> sNodeID_IDArray = new HashMap<String, List<String>>();
			HashMap<String, List<String>> eNodeID_IDArray = new HashMap<String, List<String>>();
			HashMap<Integer, RoadLink> line_RoadLink = new HashMap<Integer, RoadLink>();//对应link在mid文件中的行号，roadlink
			String s;
			while ((s = file_mid.readLine()) != null) {// 读MID文件
				line++;
				String[] s_array = s.split("\",\"|\""); // 用正则分隔这行
			
				// 判断direction 是否是双向的，即取值为2 or 3
				if (s_array[6].equals("2") || s_array[6].equals("3")) {
					highway_roadlink_id.add(s_array[2]);
					highway_lines.add(line);
					RoadLink temp_RoadLink = new RoadLink(s_array);
					temp_RoadLink.line = line;
					id_RoadLink.put(s_array[2], temp_RoadLink);
					if (sNodeID_IDArray.containsKey(temp_RoadLink.SnodeID)) {
						List<String> temp_array = sNodeID_IDArray.get(temp_RoadLink.SnodeID);
						temp_array.add(s_array[2]);
						sNodeID_IDArray.put(temp_RoadLink.SnodeID, temp_array);
					} else {
						// 此处认为某个Node对应的路链最多2个，即一条路最多分2条路。
						List<String> temp_array = new ArrayList<String>(2);
						temp_array.add(s_array[2]);
						sNodeID_IDArray.put(temp_RoadLink.SnodeID, temp_array);
					}
					if (eNodeID_IDArray.containsKey(temp_RoadLink.EnodeID)) {
						List<String> temp_array = eNodeID_IDArray.get(temp_RoadLink.EnodeID);
						temp_array.add(s_array[2]);
						eNodeID_IDArray.put(temp_RoadLink.EnodeID, temp_array);
					} else {
						// 此处认为某个Node对应的路链最多2个，即一条路最多分2条路。
						List<String> temp_array = new ArrayList<String>(2);
						temp_array.add(s_array[2]);
						eNodeID_IDArray.put(temp_RoadLink.EnodeID, temp_array);
					}
					line_RoadLink.put(line, temp_RoadLink);

				}
			}
			while ((s = file_mif.readLine()) != null) {// mif文件
				if (s.contains("Line") || s.contains("Pline")) {//开始新一轮经纬度序列
					mif_kuai++;
					lonLat_array.clear();
					lonLat_array.add(s);
				} else if (s.contains("Pen")) {//结束
					if (highway_lines.contains(mif_kuai)) {// 读到了经纬度序列
						List<LonLat> lonLat_list = new ArrayList<LonLat>();
						int count = 0;
						String lon = null, lat = null;
						for (String x : lonLat_array) {
							String[] array_x = x.split(" ");
							for (String str : array_x) {
								if(str.equals("Pline")) break;
								if (!str.contains("ine")) {
									count++;
									if (count % 2 != 0) {
										lon = str;
									} else {
										lat = str;
										lonLat_list.add(new LonLat(lon, lat));
									}
								}
							}
						}
						// 此处完成对经纬度序列的读取，接下来将其加入路链序列中。
						RoadLink temp_RoadLink = line_RoadLink.get(mif_kuai);

						temp_RoadLink.lonlat_list = lonLat_list;
						line_RoadLink.put(mif_kuai, temp_RoadLink);
						id_RoadLink.put(temp_RoadLink.ID, temp_RoadLink);
					}
				} else {
					lonLat_array.add(s);
				}
			}
			// 从这里起，为每个路链配置next属性。采用惯用手法BFS。
			ArrayDeque<RoadLink> roadLink_queue = new ArrayDeque<RoadLink>();
			while (!highway_roadlink_id.isEmpty()) {
				roadLink_queue.add(id_RoadLink.get(highway_roadlink_id.get(0)));
				while (roadLink_queue.size() != 0) {
					RoadLink loop_RoadLink = roadLink_queue.poll();
					highway_roadlink_id.remove(loop_RoadLink.ID);
					if (!loop_RoadLink.visit) {
						// System.out.println(loop_RoadLink.ID+","+loop_RoadLink.EnodeID);
						loop_RoadLink.visit = true;
						loop_RoadLink.next_ID = sNodeID_IDArray.get(loop_RoadLink.EnodeID);
						loop_RoadLink.pre_ID = eNodeID_IDArray.get(loop_RoadLink.SnodeID);
						if (loop_RoadLink.next_ID == null) {// 按诸老师说的说法新增了容错判断
							String change_EnodeID = "";
							if (loop_RoadLink.EnodeID.charAt(0) == '2') {
								change_EnodeID = "1"
										+ loop_RoadLink.EnodeID.substring(1, loop_RoadLink.EnodeID.length());
							} else if (loop_RoadLink.EnodeID.charAt(0) == '1') {
								change_EnodeID = "2"
										+ loop_RoadLink.EnodeID.substring(1, loop_RoadLink.EnodeID.length());
							}
							loop_RoadLink.next_ID = sNodeID_IDArray.get(change_EnodeID);
						}
						if (loop_RoadLink.pre_ID == null) {// 按诸老师说的说法新增了容错判断
							String change_SnodeID = "";
							if (loop_RoadLink.SnodeID.charAt(0) == '2') {
								change_SnodeID = "1"
										+ loop_RoadLink.SnodeID.substring(1, loop_RoadLink.SnodeID.length());
							} else if (loop_RoadLink.SnodeID.charAt(0) == '1') {
								change_SnodeID = "2"
										+ loop_RoadLink.SnodeID.substring(1, loop_RoadLink.SnodeID.length());
							}
							loop_RoadLink.pre_ID = eNodeID_IDArray.get(change_SnodeID);
						}
						id_RoadLink.put(loop_RoadLink.ID, loop_RoadLink);
						if (loop_RoadLink.next_ID != null) {
							for (String str_id : loop_RoadLink.next_ID) {
								RoadLink temp_RoadLink_judge = id_RoadLink.get(str_id);
								if (temp_RoadLink_judge != null)
									roadLink_queue.add(temp_RoadLink_judge);
							}
						}
						if (loop_RoadLink.pre_ID != null) {
							for (String str_id : loop_RoadLink.pre_ID) {
								RoadLink temp_RoadLink_judge = id_RoadLink.get(str_id);
								if (temp_RoadLink_judge != null)
									roadLink_queue.add(temp_RoadLink_judge);
							}
						}
					}
				}
			}
			file_mid.close();
			file_mif.close();
			return id_RoadLink;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	public static BufferedReader getReader(String in,String encoding){
		File file=new File(in);
		BufferedReader reader=null;
		try{
			InputStreamReader input=new InputStreamReader(new FileInputStream(file),encoding);
			reader=new BufferedReader(input);
		}catch(Exception e){
			e.printStackTrace();
		}
		return reader;
	}
	/**
	 * 写文件
	 * @param out 输出文件路径
	 * @param encoding	输出文件格式
	 * @return	返回BufferedWriter
	 */
	public static BufferedWriter getWriter(String out,String encoding){
		File file=new File(out);
		BufferedWriter writer=null;
		if(!file.exists()){
			try{
				OutputStreamWriter output=new OutputStreamWriter(new FileOutputStream(file),encoding);
				writer=new BufferedWriter(output);
		    }catch(Exception e){
		    	e.printStackTrace();
		    }
		}else{
			try{
				OutputStreamWriter output=new OutputStreamWriter(new FileOutputStream(file,true),encoding);
				writer=new BufferedWriter(output);
		    }catch(Exception e){
				e.printStackTrace();
	    	}	
		
		
		}
		return writer;
	}
	public static void readCMid(String path,Map<Integer,String> idWithPointMessage){
		BufferedReader reader=getReader(path,"GBK");
		String line="";
		int lineId=0;
		try{
			while((line=reader.readLine())!=null){
				lineId++;
				String[] data=line.split(",",10);
				String inLinkId=data[3].replaceAll("\"", "").trim();
				String outLinkId=data[4].replaceAll("\"", "").trim();
				String CondType=data[5].replaceAll("\"", "").trim();
				idWithPointMessage.put(lineId, inLinkId+";"+outLinkId+";"+CondType);
			}
			reader.close();
//			System.out.println("Rzhejiang2.mid:"+lineId);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static Map<String,List<String>> addGpsPointCmid(String path,Map<Integer,String> idWithPointMessage){//mid行号，进入linkID;退出linkID;CondType
		BufferedReader reader=getReader(path,"GBK");
		String line="";
		int lineId=0;
		try{
			while((line=reader.readLine())!=null){
				if(line.startsWith("Point")){
					String f="";
					lineId++;
					String[] data=line.split(" ",3);
					f+=data[1]+" "+data[2];
					String value=idWithPointMessage.get(lineId);
					value=value+";"+f;
					idWithPointMessage.put(lineId, value);
				}
			}
			reader.close();
			System.out.println("Rzhejiang2.mif:"+lineId);
		}catch(Exception e){
			e.printStackTrace();
		}
		Map<String,List<String>> mapTollCollection=new HashMap<>();
		Set setPoint=idWithPointMessage.entrySet();
		Map.Entry[] entriesPoint = (Map.Entry[])setPoint.toArray(new Map.Entry[setPoint.size()]);
		for(int i=0;i<entriesPoint.length;i++){
			String value=entriesPoint[i].getValue().toString();
			String[] v=value.split(";",4);
			String inLinkId=v[0];
			String outLinkId=v[1];
			String contentType=v[2];
			String gps=v[3];
			if(contentType.equals("3")){
				List<String> list=new ArrayList<>();
				list.add(inLinkId);
				list.add(outLinkId);
				mapTollCollection.put(gps, list);
			}
		}
		return mapTollCollection;
	}
	
	
	public static HashMap<String, RoadLink> getTopology_ID_RoadLink_RoadName_Station(String mid_file_path,
			String mif_file_path, String pname_file_path, String poi_file_path, String rlname_file_path,
			String rname_file_path) {
		HashMap<String, StationInMap> poi_StationInMap_hash = new HashMap<String, StationInMap>(); // poi的ID和名字对应
		HashMap<String, String> poi_Name_hash = new HashMap<String, String>(); // poi的ID和名字对应       把所有收费站找了出来
		HashMap<String, List<String>> id_route_id = new HashMap<String, List<String>>();
		HashMap<String, List<String>> route_id_name = new HashMap<String, List<String>>();
		HashMap<String, RoadLink> id_Roadlink = GetSimplifiedTopology.getTopology_ID_RoadLink(mid_file_path, mif_file_path);//linkID，RoadLink
		// List<String> poi_station_array = new ArrayList<String>(300); //
		// poi_name_hash的key的集合，为了方便不用iterator
		// List<String> roadlink_id_array = new ArrayList<String>(300);
		// List<String> route_id_array = new ArrayList<String>(300);
		try {
			String line = null;
			String Czhejiang2Mid="E:/G-1149/trafficCongestion/北京地图数据/beijing/road/Cbeijing.mid";
			String Czhejiang2Mif="E:/G-1149/trafficCongestion/北京地图数据/beijing/road/Cbeijing.mif";
	    	Map<Integer,String> idWithPointMessage=new HashMap<>();
	    	readCMid(Czhejiang2Mid,idWithPointMessage);//行号，进入linkID;退出linkID;CondType
	    	Map<String,List<String>> mapGpsLinkId=addGpsPointCmid(Czhejiang2Mif,idWithPointMessage);//gps(lon lat),进入linkID；退出linkID
	    	
			BufferedReader pname = new BufferedReader(new FileReader(pname_file_path));
			while ((line = pname.readLine()) != null) {
				String[] line_array = line.split("\",\"|\"");
				if (line_array[3].endsWith("收费站")) { // 以收费站结尾
					poi_Name_hash.put(line_array[1], line_array[3]); // 查找到了是收费站的POI。一一对应。
					// poi_station_array.add(line_array[1]);
				}
			}
			pname.close();
			int count=0;
			int amount=0;
			BufferedReader poi = new BufferedReader(new FileReader(poi_file_path));
			while ((line = poi.readLine()) != null) {
				String[] line_array = line.split("\",\"|\"");
				if (poi_Name_hash.containsKey(line_array[8])) {
					amount++;
					String gps=line_array[6]+" "+line_array[7];
					if(mapGpsLinkId.containsKey(gps)){
						count++;
						List<String> list=mapGpsLinkId.get(gps);
						for(int i=0;i<list.size();i++){
							String linkId=list.get(i);
							StationInMap loop_station=new StationInMap(line_array[8], poi_Name_hash.get(line_array[8]));
							loop_station.lonLat=new LonLat(line_array[6], line_array[7]);
							loop_station.roadlink_ID = linkId;
							poi_StationInMap_hash.put(loop_station.poi_ID, loop_station);
							RoadLink station_RoadLink = id_Roadlink.get(loop_station.roadlink_ID);
							if(station_RoadLink!=null){
								station_RoadLink.station = loop_station.name;
								id_Roadlink.put(station_RoadLink.ID, station_RoadLink);
							}
						}
					}
//					StationInMap loop_station = poi_StationInMap_hash.get(line_array[8]);
//					loop_station.lonLat = new LonLat(line_array[6], line_array[7]);
//					String gps=line_array[6]+" "+line_array[7];
//					if(mapGpsLinkId.containsKey(gps)){
//						count++;
//						List<String> list=mapGpsLinkId.get(gps);
//						for(int i=0;i<list.size();i++){
//							String linkId=list.get(i);
//							loop_station.roadlink_ID = linkId;
//							poi_StationInMap_hash.put(loop_station.poi_ID, loop_station);
//							RoadLink station_RoadLink = id_Roadlink.get(loop_station.roadlink_ID);
//							if(station_RoadLink!=null){
//								station_RoadLink.station = loop_station.name;
//								id_Roadlink.put(station_RoadLink.ID, station_RoadLink);
//							}
//						}
//					}
				}
			}
			poi.close();
			System.out.println("count:"+count);
			System.out.println("amount:"+amount);
			
			BufferedReader rlname = new BufferedReader(new FileReader(rlname_file_path));
			while ((line = rlname.readLine()) != null) {
				String[] line_array = line.split("\",\"|\"");
				// if (roadlink_id_array.contains(line_array[2])) {
				// route_id_array.add(line_array[3]);
				// }
				if (id_route_id.containsKey(line_array[2])) {
					List<String> loop_route_id = id_route_id.get(line_array[2]);
					loop_route_id.add(line_array[3]);
					id_route_id.put(line_array[2], loop_route_id);
				} else {
					List<String> loop_route_id = new ArrayList<String>(5);
					loop_route_id.add(line_array[3]);
					id_route_id.put(line_array[2], loop_route_id);
				}
			}
			rlname.close();
			BufferedReader rname = new BufferedReader(new FileReader(rname_file_path));
			while ((line = rname.readLine()) != null) {
				String[] line_array = line.split("\",\"|\"");
				if (line_array[14].equals("1")) { // 如果此处是中文路名
					if (route_id_name.containsKey(line_array[1])) {
						List<String> loop_name = route_id_name.get(line_array[1]);
						loop_name.add(fullWidth2halfWidth(line_array[3]));
						route_id_name.put(line_array[1], loop_name);
					} else {
						List<String> loop_name = new ArrayList<String>(5);
						loop_name.add(fullWidth2halfWidth(line_array[3]));
						route_id_name.put(line_array[1], loop_name);
					}
				}
			}
			rname.close();
			for (Iterator<RoadLink> iterator = id_Roadlink.values().iterator(); iterator.hasNext();) {
				RoadLink loop_roadlink = iterator.next();
				if (id_route_id.containsKey(loop_roadlink.ID)) {
					List<String> roadlink_name = new ArrayList<String>();
					for (String route_ID : id_route_id.get(loop_roadlink.ID)) {
						try{
							if(route_id_name.containsKey(route_ID))
								roadlink_name.addAll(route_id_name.get(route_ID));
						}
						catch(Exception e){
							e.printStackTrace();
							continue;
						}
						
					}
					loop_roadlink.highway_ID = StationInMap.listAddSeparator(roadlink_name);
				}
				id_Roadlink.put(loop_roadlink.ID, loop_roadlink);
			}
			return id_Roadlink;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 全角半角转换
	 * @param fullWidthStr
	 * @return
	 */
	public static String fullWidth2halfWidth(String fullWidthStr) {
		if (null == fullWidthStr || fullWidthStr.length() <= 0) {
			return "";
		}
		char[] charArray = fullWidthStr.toCharArray();
		// 对全角字符转换的char数组遍历
		for (int i = 0; i < charArray.length; ++i) {
			int charIntValue = (int) charArray[i];
			// 如果符合转换关系,将对应下标之间减掉偏移量65248;如果是空格的话,直接做转换
			if (charIntValue >= 65281 && charIntValue <= 65374) {
				charArray[i] = (char) (charIntValue - 65248);
			} else if (charIntValue == 12288) {
				charArray[i] = (char) 32;
			}
		}
		return new String(charArray);
	}
	
	/**
	 * 根据四维地图的mid文件，得到该文件内的所有图幅号，用Set形式表达
	 * @param mid_file_path
	 * @return
	 */
	public static Set<String> getMapSheetNumberSet(String mid_file_path){
		try {
			Set<String> MapSheetNumberSet=new HashSet<String>();
			BufferedReader reader=new BufferedReader(new FileReader(mid_file_path));
			String line;
			while((line=reader.readLine())!=null){
				String[] line_array = line.split("\",\"|\"");
				MapSheetNumberSet.add(line_array[1]);
			}
			reader.close();
			return MapSheetNumberSet;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
	}
}


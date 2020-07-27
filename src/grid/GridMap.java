package grid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLNonTransientConnectionException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * 
 * @author 98259
 *获取每条link的经纬度序列方法在 GetSimplifiedTopology.java类中
 */
public class GridMap {
    private static final double EARTH_RADIUS = 6378137;
    private static final int gridUnit = 100;//网格边长

	public static void main(String[] args) {
		String lonLatPath = "E:/G-1149/trafficCongestion/网格化/LonLat.csv";
		String oriMapPath = "E:/G-1149/trafficCongestion/网格化/originMap.csv";
		String linkAttrPath = "E:/G-1149/trafficCongestion/网格化/linkAttr.csv";
		String linkNumPath = "E:/G-1149/trafficCongestion/网格化/linkNum.csv";//link,link经过的网格编号
		String gridNumPath = "E:/G-1149/trafficCongestion/网格化/gridNum.csv";//网格编号，网格里有的link
		String _02LinkMapPath = "E:/G-1149/trafficCongestion/网格化/02LinkMap.csv";
		String linkStatusPath = "E:/G-1149/trafficCongestion/网格化/linkStatus_13_完整.csv";
		String gridLinkPeerPath = "E:/G-1149/trafficCongestion/网格化/gridLinkPeer.csv";
		
		//getLinkAttr(lonLatPath, oriMapPath, linkAttrPath);//获得所需的link属性
		//getLinkGridNum(lonLatPath, linkNumPath);
		//getGridNumLink(linkNumPath, gridNumPath);
		
		getGridLinkPeer(gridNumPath, _02LinkMapPath, linkAttrPath, linkStatusPath, gridLinkPeerPath);

	}
	
	public static void getGridLinkPeer(String gridNumPath, String _02LinkMapPath, String linkAttrPath, String linkStatusPath, String gridLinkPeerPath){
		HashMap<String, String> linkAttrMap = getLinkAttrMap(linkAttrPath);
		HashSet<String> _02LinkSet = getLinkSet(_02LinkMapPath);
		HashSet<String> linkStatusSet = getLinkSet(linkStatusPath);
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(gridNumPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(gridLinkPeerPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				int len = lineArr.length;
				String gridNum = lineArr[0];
				HashMap<String, HashMap<String, String>> gridMap = new HashMap<>();
				for(int i = 1; i < len; i++){
					String linkID = lineArr[i];
					if(!_02LinkSet.contains(linkID) || !linkStatusSet.contains(linkID)) continue;
					String dir = linkAttrMap.get(linkID).split(",")[0];
					String kind = linkAttrMap.get(linkID).split(",")[1];
					if(gridMap.containsKey(dir)){
						HashMap<String, String> kindMap = gridMap.get(dir);
						if(!kindMap.containsKey(kind)){//有可能好几条link有相同的dir和kind，但我们只需要一条就可，后续可在此做容错判断
							kindMap.put(kind, linkID);
						}
						gridMap.put(dir, kindMap);
					}else{
						HashMap<String, String> kindMap = new HashMap<>();
						kindMap.put(kind, linkID);
						gridMap.put(dir, kindMap);
					}
				}
				
				writer.write(gridNum);
				if(gridMap.containsKey("0") && gridMap.containsKey("2")){
					HashMap<String, String> _0Map = gridMap.get("0");
					HashMap<String, String> _2Map = gridMap.get("2");
					for(String key1: _0Map.keySet()){
						for(String key2: _2Map.keySet()){
							if(key1.equals(key2)) writer.write("," + _0Map.get(key1) + "," + _2Map.get(key2));
						}
					}
				}
				if(gridMap.containsKey("1") && gridMap.containsKey("3")){
					HashMap<String, String> _1Map = gridMap.get("1");
					HashMap<String, String> _3Map = gridMap.get("3");
					for(String key1: _1Map.keySet()){
						for(String key2: _3Map.keySet()){
							if(key1.equals(key2)) writer.write("," + _1Map.get(key1) + "," + _3Map.get(key2));
						}
					}
				}
				writer.write("\n");
			}
			
			reader.close();
			writer.flush();
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(gridLinkPeerPath + " 写文件结束");
	}
	
	public static HashMap<String, String> getLinkAttrMap(String path){
		HashMap<String, String> map = new HashMap<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				map.put(lineArr[0], lineArr[1] + "," + lineArr[3]);//方向+种别代码
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	public static HashSet<String> getLinkSet(String path){
		HashSet<String> set = new HashSet<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				set.add(lineArr[0]);//linkID
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return set;
	}
	
	public static void getGridNumLink(String inPath, String outPath){
		HashMap<String, String> gridMap = new HashMap<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				int len = lineArr.length;
				String linkID = lineArr[0];
				
				for(int i = 1; i < len; i++){
					String gridNum = lineArr[i];
					if(gridMap.containsKey(gridNum)){
						String linkList = gridMap.get(gridNum);
						linkList += ("," + linkID); 
						gridMap.put(gridNum, linkList);
					}else{
						gridMap.put(gridNum, linkID);
					}
				}
			}
			
			reader.close();
			
			writeData(gridMap, outPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(outPath + " 写文件结束");
	}
	
	public static void writeData(HashMap<String, String> map, String path){
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(path), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			for(String key: map.keySet()){
				writer.write(key + "," + map.get(key));
				writer.write("\n");
			}
			
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * 按照每条link的经纬度计算每条link都经过哪些网格
	 * @param inPath LonLat.csv
	 * @param outPath linkNum.csv 组织格式： linkID，网格编号list
	 */
	public static void getLinkGridNum(String inPath, String outPath){
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				String linkID = lineArr[0];
				int size = lineArr.length;
				ArrayList<String> numSet = new ArrayList<>();
				
				for(int i = 1; i < size-1; i++){
					Double[] start = {Double.parseDouble(lineArr[i].split(":")[0]),Double.parseDouble(lineArr[i].split(":")[1])};
					Double[] end = {Double.parseDouble(lineArr[i+1].split(":")[0]),Double.parseDouble(lineArr[i+1].split(":")[1])};
					int dis = (int)getDistance(start[0], start[1], end[0], end[1]);
					if(dis > 2*gridUnit){
						ArrayList<String> gpsList = calInterCoor(lineArr[i], lineArr[i+1], (dis/gridUnit)+1);
						for(int j = 0; j < gpsList.size(); j++){
							String num = getXY(gpsList.get(j).replace(":", ",")).split(",")[0];
							if(!numSet.contains(num)) numSet.add(num);
						}
					}else{
						String num = getXY(lineArr[i].replace(":", ",")).split(",")[0];
						if(!numSet.contains(num)) numSet.add(num);
					}
				}
				
				String num = getXY(lineArr[size-1].replace(":", ",")).split(",")[0];
				if(!numSet.contains(num)) numSet.add(num);

				String numList = "";
				for(String str : numSet) numList += ("," + str);
				writer.write(linkID + numList + "\n");
			}
			
			reader.close();
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(outPath + " 写文件结束");
	}

	//对于长度很长但是仅有起止点经纬度的link，按长度拆分，计算中间GPS
	public static ArrayList<String> calInterCoor(String start, String end, int len){
		ArrayList<String> gpsList = new ArrayList<>();
		Double s_lon = Double.parseDouble(start.split(":")[0]);
		Double s_lat = Double.parseDouble(start.split(":")[1]);
		Double e_lon = Double.parseDouble(end.split(":")[0]);
		Double e_lat = Double.parseDouble(end.split(":")[1]);
		Double lonUnit = (e_lon-s_lon)/len;
		Double latUnit = (e_lat-s_lat)/len;
		
		for(int i = 0; i < len; i++){
			gpsList.add((s_lon + (i * lonUnit)) + ":" + (s_lat + (i * latUnit)));
		}
		gpsList.add(end);
		return gpsList;
	}
	
	/**
	 * 获得link的实际方向，种别代码数，种别代码，中心点坐标
	 * @param lonLatPath	link的经纬度序列
	 * @param oriMapPath	Rmid文件简化 originMap.csv
	 * @param outPath	结果文件，字段说明：linkID，实际方向，种别代码数，种别代码，道路长度，中心坐标（lon,lat）,所属网格编号，坐标
	 */
	public static void getLinkAttr(String lonLatPath, String oriMapPath, String outPath){
		DecimalFormat df = new DecimalFormat("#.0000");
		HashMap<String, String> oriMap = getOriMap(oriMapPath);
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(lonLatPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				
				double lon1 = Double.parseDouble(lineArr[1].split(":")[0]);
				double lat1 = Double.parseDouble(lineArr[1].split(":")[1]);
				double lon2 = Double.parseDouble(lineArr[2].split(":")[0]);
				double lat2 = Double.parseDouble(lineArr[2].split(":")[1]);
				int dir = -1;
				if(lon1<=lon2 && lat1<=lat2) dir = 0;
				if(lon1>=lon2 && lat1<=lat2) dir = 1;
				if(lon1>=lon2 && lat1>=lat2) dir = 2;
				if(lon1<=lon2 && lat1>=lat2) dir = 3;
				
				String linkID = lineArr[0];
				int len = lineArr.length;
				String coor = "";
				if(len == 3){//该link只有起止点经纬度,取算术平均作为中心点坐标
					String lon = df.format((lon1 + lon2)/2);
					String lat = df.format((lat1 + lat2)/2);
					coor = lon + "," + lat;
				}else if(len > 3){
					int cen = (len+1)/2;
					coor = lineArr[cen].replace(":", ",");
				}else{
					System.out.println(lineArr[0]);
				}
				
				writer.write(linkID+","+dir+","+oriMap.get(linkID)+","+coor+","+getXY(coor)+ "\n");
			}
			
			reader.close();
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(outPath + " 写文件结束");
	}
	
	public static HashMap<String, String> getOriMap(String path){
		HashMap<String, String> linkType = new HashMap<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				linkType.put(lineArr[0], lineArr[1]+","+lineArr[2]+","+lineArr[7]);
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return linkType;
	}

    //转化为弧度
    private static double rad(double d) {
        return (d * Math.PI / 180.0);
    }
    
    //返回数据以米为单位
    public static double getDistance(double lng1, double lat1, double lng2, double lat2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = (2 * Math.asin(
                Math.sqrt(
                        Math.pow(Math.sin(a / 2), 2)
                                + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)
                )
        ));
        s = (s * EARTH_RADIUS);
        //s = Math.round(s * 10000) / 10000;

        return s;
    }

    /**
     * 根据link坐标和北京左下角经纬度，计算link坐标和网格编号
     * @param coor lng+","+lat
     * @param d 
     * @return	网格编号 + x,y
     */
    public static String getXY(String coor) {
    	String[] arr = coor.split(",");
    	double lng = Double.parseDouble(arr[0]);
    	double lat = Double.parseDouble(arr[1]);
    	
    	if(lat<39.416670 || lat>41.08333 || lng<115.375000 || lng>117.5) return "error";
    	
        double[] a = {115.375000,39.416670};
        double bLng = a[0];
        double bLat = a[1];
        int x = (int) ((getDistance(bLng, lat, lng, lat) / gridUnit) + 1);
        int y = (int) ((getDistance(lng, bLat, lng, lat) / gridUnit) + 1);
        
        int gridNum = 1783*(y-1)+x;
        
        return gridNum + "," + x + "," + y;
    }
}

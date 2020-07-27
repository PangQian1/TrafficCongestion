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
 *��ȡÿ��link�ľ�γ�����з����� GetSimplifiedTopology.java����
 */
public class GridMap {
    private static final double EARTH_RADIUS = 6378137;
    private static final int gridUnit = 100;//����߳�

	public static void main(String[] args) {
		String lonLatPath = "E:/G-1149/trafficCongestion/����/LonLat.csv";
		String oriMapPath = "E:/G-1149/trafficCongestion/����/originMap.csv";
		String linkAttrPath = "E:/G-1149/trafficCongestion/����/linkAttr.csv";
		String linkNumPath = "E:/G-1149/trafficCongestion/����/linkNum.csv";//link,link������������
		String gridNumPath = "E:/G-1149/trafficCongestion/����/gridNum.csv";//�����ţ��������е�link
		String _02LinkMapPath = "E:/G-1149/trafficCongestion/����/02LinkMap.csv";
		String linkStatusPath = "E:/G-1149/trafficCongestion/����/linkStatus_13_����.csv";
		String gridLinkPeerPath = "E:/G-1149/trafficCongestion/����/gridLinkPeer.csv";
		
		//getLinkAttr(lonLatPath, oriMapPath, linkAttrPath);//��������link����
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
						if(!kindMap.containsKey(kind)){//�п��ܺü���link����ͬ��dir��kind��������ֻ��Ҫһ���Ϳɣ��������ڴ����ݴ��ж�
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
		System.out.println(gridLinkPeerPath + " д�ļ�����");
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
				map.put(lineArr[0], lineArr[1] + "," + lineArr[3]);//����+�ֱ����
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
		System.out.println(outPath + " д�ļ�����");
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
	 * ����ÿ��link�ľ�γ�ȼ���ÿ��link��������Щ����
	 * @param inPath LonLat.csv
	 * @param outPath linkNum.csv ��֯��ʽ�� linkID��������list
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
		System.out.println(outPath + " д�ļ�����");
	}

	//���ڳ��Ⱥܳ����ǽ�����ֹ�㾭γ�ȵ�link�������Ȳ�֣������м�GPS
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
	 * ���link��ʵ�ʷ����ֱ���������ֱ���룬���ĵ�����
	 * @param lonLatPath	link�ľ�γ������
	 * @param oriMapPath	Rmid�ļ��� originMap.csv
	 * @param outPath	����ļ����ֶ�˵����linkID��ʵ�ʷ����ֱ���������ֱ���룬��·���ȣ��������꣨lon,lat��,���������ţ�����
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
				if(len == 3){//��linkֻ����ֹ�㾭γ��,ȡ����ƽ����Ϊ���ĵ�����
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
		System.out.println(outPath + " д�ļ�����");
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

    //ת��Ϊ����
    private static double rad(double d) {
        return (d * Math.PI / 180.0);
    }
    
    //������������Ϊ��λ
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
     * ����link����ͱ������½Ǿ�γ�ȣ�����link�����������
     * @param coor lng+","+lat
     * @param d 
     * @return	������ + x,y
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

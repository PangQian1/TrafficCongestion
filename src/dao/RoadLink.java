package dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import util.Config;

public class RoadLink {
	public int speed_upbound, speed_lowbound, lane, direction, line, stake_direction = 0;
	public double length, stake_start = 0, stake_end = 0, station_stake = 0, tachometer_stake = 0;
	public boolean isRamp, visit = false;
	public char road_attribute;
	public String ID, highway_ID = null, SnodeID, EnodeID, station = null, tachometer = null;
	public List<LonLat> lonlat_list;
	public List<String> next_ID, pre_ID;

	public RoadLink(String line) {
		String[] line_array = line.split("\",\"|\"");
		int[] speed = RoadLink.speed_judge(Integer.valueOf(line_array[25]));
		speed_lowbound = speed[0];
		speed_upbound = speed[1];
		direction = Integer.valueOf(line_array[6]);
		lane = RoadLink.lane_num(line_array[26], line_array[27]);
		length = Double.valueOf(line_array[13]);
		ID = line_array[2];
		SnodeID = line_array[RoadLink.judge_SNode(direction)];
		EnodeID = line_array[RoadLink.judge_ENode(direction)];
		isRamp = RoadLink.judge_Ramp(line_array[4]);
		road_attribute = RoadLink.judge_Road_attribute(line_array[4]);
	}

	public RoadLink(String[] line_array) {
		int[] speed = RoadLink.speed_judge(Integer.valueOf(line_array[25]));
		speed_lowbound = speed[0];
		speed_upbound = speed[1];
		lane = RoadLink.lane_num(line_array[26], line_array[27]);
		length = Double.valueOf(line_array[13]);
		direction = Integer.valueOf(line_array[6]);
		ID = line_array[2];
		SnodeID = line_array[RoadLink.judge_SNode(direction)];
		EnodeID = line_array[RoadLink.judge_ENode(direction)];
		isRamp = RoadLink.judge_Ramp(line_array[4]);
		road_attribute = RoadLink.judge_Road_attribute(line_array[4]);
	}

	public RoadLink(String[] line_array, boolean forRoadLinkMidFile) {
		// TODO Auto-generated constructor stub
		// 这个是为中间文件RoadLink设计的针对toString方法设计的逆函数
		ID = line_array[0];
		next_ID = line_array[1].equals("") ? null : Arrays.asList(line_array[1].split("#"));
		pre_ID = line_array[2].equals("") ? null : Arrays.asList(line_array[2].split("#"));
		length = Double.valueOf(line_array[3]);
		isRamp = Boolean.valueOf(line_array[4]);
		road_attribute = line_array[5].charAt(0);
		highway_ID = line_array[6];
		lonlat_list = new ArrayList<LonLat>(1);
		lonlat_list.add(new LonLat(line_array[7], line_array[8]));
		lane = Integer.valueOf(line_array[9]);
		speed_lowbound = Integer.valueOf(line_array[10]);
		speed_upbound = Integer.valueOf(line_array[11]);
		line = Integer.valueOf(line_array[12]);
		tachometer = line_array[13].equals("null") ? null : line_array[13];
		tachometer_stake = Double.valueOf(line_array[14]);
		station = line_array[15].equals("null") ? null : line_array[15];
		station_stake = Double.valueOf(line_array[16]);
		stake_start = Double.valueOf(line_array[17]);
		stake_end = Double.valueOf(line_array[18]);
		stake_direction = Integer.valueOf(line_array[19]);
	}

	public RoadLink(RoadLink obj) {
		speed_upbound = obj.speed_upbound;
		speed_lowbound = obj.speed_lowbound;
		lane = obj.lane;
		direction = obj.direction;
		line = obj.line;
		stake_direction = obj.stake_direction;
		length = obj.length;
		stake_start = obj.stake_start;
		stake_end = obj.stake_end;
		station_stake = obj.station_stake;
		tachometer_stake = obj.tachometer_stake;
		isRamp = obj.isRamp;
		visit = obj.visit;
		road_attribute = obj.road_attribute;
		ID = obj.ID;
		highway_ID = obj.highway_ID;
		SnodeID = obj.SnodeID;
		EnodeID = obj.EnodeID;
		station = obj.station;
		tachometer = obj.tachometer;
		lonlat_list = obj.lonlat_list == null ? null : new ArrayList<LonLat>(obj.lonlat_list);
		next_ID = obj.next_ID == null ? null : new ArrayList<String>(obj.next_ID);
		pre_ID = obj.pre_ID == null ? null : new ArrayList<String>(obj.pre_ID);
	}

	public static int judge_SNode(int direction) {
		if (direction == 2) // 顺行
			return 10;
		else // 逆行
			return 11;
	}

	public static int judge_ENode(int direction) {
		if (direction == 2)
			return 11;
		else
			return 10;
	}

	public static boolean judge_Ramp(String kind) {
		if (kind.contains("000b")) {
			return true;
		}
		return false;
	}

	public static char judge_Road_attribute(String kind) {
		String[] kind_array = kind.split("");
		if (kind.length() < 5) {
			return RoadLink.judge_Road_char(kind_array[3]);
		} else if (kind.length() >= 5 && kind.length() < 10) {
			return RoadLink.judge_Road_char(kind_array[3], kind_array[8]);
		} else if (kind.length() >= 10 && kind.length() < 15) {
			return RoadLink.judge_Road_char(kind_array[3], kind_array[8], kind_array[13]);
		}
		return 'G';
	}

	public static char judge_Road_char(String str) {
		if (str.equals("6"))
			return 'P';
		else if (str.equals("7"))
			return 'F';
		else if (str.equals("f"))
			return 'T';
		else if (str.equals("5"))
			return 'I';
		else if (str.equals("3"))
			return 'J';
		return 'G';
	}

	public static char judge_Road_char(String str1, String str2) {
		char ch1 = RoadLink.judge_Road_char(str1), ch2 = RoadLink.judge_Road_char(str2);
		if (ch1 == 'P' || ch2 == 'P')
			return 'P';
		if (ch1 == 'F' || ch2 == 'F')
			return 'F';
		if (ch1 == 'T' || ch2 == 'T')
			return 'T';
		if (ch1 == 'I' || ch2 == 'I')
			return 'I';
		if (ch1 == 'J' || ch2 == 'J')
			return 'J';
		return 'G';
	}

	public static char judge_Road_char(String str1, String str2, String str3) {
		char ch1 = RoadLink.judge_Road_char(str1), ch2 = RoadLink.judge_Road_char(str2);
		char ch3 = RoadLink.judge_Road_char(str3);
		if (ch1 == 'P' || ch2 == 'P' || ch3 == 'P')
			return 'P';
		if (ch1 == 'F' || ch2 == 'F' || ch3 == 'F')
			return 'F';
		if (ch1 == 'T' || ch2 == 'T' || ch3 == 'T')
			return 'T';
		if (ch1 == 'I' || ch2 == 'I' || ch3 == 'I')
			return 'I';
		if (ch1 == 'J' || ch2 == 'J' || ch3 == 'J')
			return 'J';
		return 'G';
	}

	public static int lane_num(String s2e, String e2s) {
		if (s2e.equals(""))
			return Integer.valueOf(e2s);
		else if (e2s.equals(""))
			return Integer.valueOf(s2e);
		else
			return 0;
	}

	public static int[] speed_judge(int speedClass) {
		int[] speed = new int[2];
		switch (speedClass) {
		case 1:
			speed[0] = 130;
			speed[1] = Config.SPEED_UPBOUND;
			break;
		case 2:
			speed[0] = 100;
			speed[1] = 130;
			break;
		case 3:
			speed[0] = 90;
			speed[1] = 100;
			break;
		case 4:
			speed[0] = 70;
			speed[1] = 90;
			break;
		case 5:
			speed[0] = 50;
			speed[1] = 70;
			break;
		case 6:
			speed[0] = 30;
			speed[1] = 50;
			break;
		case 7:
			speed[0] = 11;
			speed[1] = 30;
			break;
		case 8:
			speed[0] = Config.SPEED_LOWBOUND;
			speed[1] = 11;
			break;
		}
		return speed;
	}

	@Override
	// 输出格式为
	// ID,下一ID，上一ID，长度，是否匝道，道路类型，所属高速编号，起点经度、起点纬度、车道数，最低限速，最高限速，mid文件中行号，测速仪，测速仪桩号，收费站，收费站桩号，桩号开始，桩号结束，桩号方向。
	public String toString() {
		StringBuilder return_str = new StringBuilder(ID);
		StringBuilder next_ID_list = new StringBuilder("");
		StringBuilder pre_ID_list = new StringBuilder("");
		if (next_ID != null) {
			int length = next_ID.size();
			for (int i = 0; i < length - 1; i++) {
				next_ID_list.append(next_ID.get(i)).append("#");
			}
			next_ID_list.append(next_ID.get(length - 1));
		}
		if (pre_ID != null) {
			int length = pre_ID.size();
			for (int i = 0; i < length - 1; i++) {
				pre_ID_list.append(pre_ID.get(i)).append("#");
			}
			pre_ID_list.append(pre_ID.get(length - 1));
		}
		return_str.append(",").append(next_ID_list.toString()).append(",").append(pre_ID_list.toString()).append(",")
				.append(String.valueOf(length)).append(",").append(String.valueOf(isRamp)).append(",")
				.append(String.valueOf(road_attribute)).append(",").append(String.valueOf(highway_ID)).append(",")
				.append(getFirstLonLat().toString()).append(",").append(String.valueOf(lane)).append(",")
				.append(String.valueOf(speed_lowbound)).append(",").append(String.valueOf(speed_upbound)).append(",")
				.append(String.valueOf(line)).append(",").append(String.valueOf(tachometer)).append(",")
				.append(String.valueOf(tachometer_stake)).append(",").append(String.valueOf(station)).append(",")
				.append(String.valueOf(station_stake)).append(",").append(String.valueOf(stake_start)).append(",")
				.append(String.valueOf(stake_end)).append(",").append(String.valueOf(stake_direction));
		return return_str.toString();
	}

	public String toString_Short() {   //这个函数返回简短版的路链性质，主要是为了突出同一条高速公路编号下的所有路链之和
		StringBuilder return_str = new StringBuilder(ID);
		return_str.append(",").append(length).append(",").append(highway_ID).append(",")
				.append(lonlat_list.get(0).toString());
		return return_str.toString();
	}
	
	public boolean is_MainLine(){
		//此函数的意思是，是否是干线。此处定义干线为：G、T、J。其余的停车区P，收费站I，服务区F为支线。
		if(road_attribute=='P'||road_attribute=='I'||road_attribute=='F')
			return false;
		return true;
	}
	
	public LonLat getFirstLonLat(){
		if(direction==3){
			int temp=lonlat_list.size();
			return lonlat_list.get(temp-1);
		}
		else{
			return lonlat_list.get(0);
		}
	}
}

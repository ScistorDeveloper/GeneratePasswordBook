package com.scistor.process.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2017/11/13.
 */
public class Test {

	public static void main(String[] args) {

		Map<String, String> map1 = new HashMap<String, String>();
		map1.put("username", "zs");

		Map<String, String> map2 = new HashMap<String, String>();
		map2.put("username", "ls");

		Set<Map<String, String>> set = new HashSet<Map<String, String>>();
		set.add(map1);
		System.out.println(set);
		set.add(map2);
		System.out.println(set);

	}

}

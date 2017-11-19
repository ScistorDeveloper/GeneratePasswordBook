package com.scistor.process.utils;

import kafka.admin.TopicCommand;

/**
 * Created by Administrator on 2017/11/10.
 */
public class TopicUtil implements RunningConfig {

	/****
	 * 查询所有主题
	 *
	 * @param zookeeperAddr
	 */
	public static void queryTopics(String zookeeperAddr) {
		String[] options = new String[] {
				"--list",
				"--zookeeper", zookeeperAddr
		};
		TopicCommand.main(options);
	}

	/****
	 * 创建主题
	 *
	 * @param zookeeperAddr
	 * @param topicName
	 */
	public static void createTopic(String zookeeperAddr, String topicName) {
		String[] options = new String[] {
				"--create",
				"--zookeeper", zookeeperAddr,
				"--partitions", "3",
				"--topic", topicName,
				"--replication-factor", "1"
		};
		TopicCommand.main(options);
	}

	/****
	 * 删除主题
	 *
	 * @param zookeeperAddr
	 * @param topicName
	 *            --delete --zookeeper host:port --topic topicname
	 */
	public static void delTopic(String zookeeperAddr, String topicName) {
		String[] options = new String[] {
				"--delete",
				"--zookeeper", zookeeperAddr,
				"--topic", topicName };
		TopicCommand.main(options);
	}

	/****
	 * 查询指定主题
	 *
	 * @param zookeeperAddr
	 * @param topicName
	 */
	public static void detailTopic(String zookeeperAddr, String topicName) {
		String[] options = new String[] {
				"--describe",
				"--zookeeper", zookeeperAddr,
				"--topic", topicName, };
		TopicCommand.main(options);
	}

	public static void main(String[] args) {

	}

}

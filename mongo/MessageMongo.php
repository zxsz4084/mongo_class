<?php
require_once 'MongoTool.php';
class MessageMongo extends MongoTool {

	static private $SERVER_INDEX = MESSAGE_MONGO;

	public function __construct() {
		parent::__construct();
		$this->connect(self::$SERVER_INDEX);
	}

	// 检测连接与库
	public function selectDB($db) {
		parent::setCurrentDB(self::$SERVER_INDEX, $db);
		return $this;
	}


}
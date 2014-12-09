<?php
class MongoBase
{
	static private $mongoSource = array();
	static private $CURRENT_DB = array();
	static private $CI = NULL;
	static private $CONFIG = NULL;
	static $retry_times = 2;
	static $try_times = 0;

	public function __construct(){
		self::$CI = &get_instance();
	}

	private function getConfig($index) {
		if (!self::$CONFIG) {
			self::$CI->load->config('mongo');
			self::$CI->load->helper('common');
			self::$CONFIG = self::$CI->config->item('mongo');
		}
		return isset(self::$CONFIG[$index]) ? self::$CONFIG[$index] : false;
	}

	public function connect($server_index){
		if (isset(self::$mongoSource[$server_index])) return;
		
		$config = $this->getConfig($server_index);
		if (!$config) return false;
		
		try {
			self::$try_times++;
			
			$host = implode(',', $config['hosts']);
			$options = array('readPreference'=> MongoClient::RP_SECONDARY_PREFERRED);
			if (isset($config['replicaSet']) && $config['replicaSet']) $options['replicaSet'] = $config['replicaSet'];

			self::$mongoSource[$server_index] = new MongoClient('mongodb://'.$host, $options);
		} catch (MongoConnectionException $e) {
			if (self::$try_times > 1) {
				alert("failed to connect mongodb: ".$host);
				error_log($e->getMessage());
			}
			
			// 集群环境出现异常时，需要给予重连的机会,来刷新线程池
			if (self::$try_times < self::$retry_times) return $this->connect($server_index); 
			return false;
		}
	}


	/**
	 * 选择要操作的库
	 * @param unknown $db
	 */
	public function selectDB($server_index, $db){
		if (isset(self::$CURRENT_DB[$server_index]) && self::$CURRENT_DB[$server_index]['name'] == $db) return;
		if (!isset(self::$mongoSource[$server_index])) return;
		self::$CURRENT_DB[$server_index]['db'] = self::$mongoSource[$server_index]->selectDB($db);
		self::$CURRENT_DB[$server_index]['name'] = $db;
	}

	public function checkDBEnable($server_index, $db ='') {
		return isset(self::$CURRENT_DB[$server_index])
				&& ($db ? (self::$CURRENT_DB[$server_index] == $db ? true : false) :true) 
				? true : false; 
	}
	
	public function getDB($server_index) {
		return self::$CURRENT_DB[$server_index]['db'];
	}

	/**
	 * 建立索引
	 * @param string		$collection_name	collection名
	 * @param string/array	$index				要建索引的字段: datetime、array('datetime'=> 1)、array('loc'=> '2d')
	 * 索引类型 2d、2dsphere... 默认创建顺序索引 1:正序 ,-1:倒序
	 * 参考 http://docs.mongodb.org/manual/administration/indexes/
	 * @param array			$index_params		相关可选参数	array('unique'=> 1, 'dropDups'=> 1, 'timeout'=> 3600,
	 * 																'sparse'=> 1, 'expireAfterSeconds'=> 86400)
	 * @return boolean
	 */
	public function ensureIndex($server_index, $collection_name, $index, $params=array('background'=> true)) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)
					->selectCollection($collection_name)
					->ensureIndex($index, $params);
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
		return true;
	}

	/**
	 * 删除索引
	 * @param string	$collection_name
	 * @param array		$index				array('index-name' => 1)
	 * @return boolean
	 */
	public function dropIndex($server_index, $collection_name, $index) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->selectCollection($collection_name)->deleteIndex($index);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	public function searchIndex($server_index, $collection_name) {
		if (!$this->checkDBEnable($server_index)) return false;
		return $this->getDB($server_index)->selectCollection($collection_name)->getIndexInfo();
	}


	/**
	 * 插入新数据
	 * @param unknown $collection_name
	 * @param unknown $data
	 * @return boolean
	 */
	public function insert($server_index, $collection_name, $data) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$collection_name->insert($data);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	
	/**
	 * 批量插入新数据
	 * @param unknown $collection_name
	 * @param unknown $data
	 * @return boolean
	 */
	public function batchInsert($server_index, $collection_name, $data) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$collection_name->batchInsert($data);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	/**
	 * 类似mysql replace into ,需要指定 _id
	 * @param string	$collection_name
	 * @param array		$data
	 */
	public function save($server_index, $collection_name, $data, $options=array()) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->selectCollection($collection_name)->save($data, $options);
			return true;
		}  catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}


	/**
	 * 删除记录
	 * @param string 	$collection_name
	 * @param array		$condition
	 * @param array		$options			参数选项 (默认只删一行)
	 */
	public function delete($server_index, $collection_name, $condition, $options=array('justOne'=> true)) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$collection_name->remove($condition, $options);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	/**
	 * 更新记录
	 * @param string	$colletion_name
	 * @param array		$condition
	 * @param array		$new_data
	 * @param array		$options		参数选项 (默认只更新一条记录)
	 */
	public function update($server_index, $colletion_name, $condition, $new_data, $options=array('multiple'=> false)) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$colletion_name->update($condition, array('$set'=> $new_data), $options);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	public function dataIncrease($server_index, $colletion_name, $condition, $increase) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$colletion_name->update($condition, $increase);
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}

	}

	public function unsetFields($server_index, $colletion_name, $condition, $unset_fields) {
		if (!$this->checkDBEnable($server_index)) return false;
		try {
			$this->getDB($server_index)->$colletion_name->update($condition, array('$unset'=> $unset_fields), array('multiple'=> true));
			return true;
		} catch (MongoCursorException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
	}

	/**
	 * 查询
	 * @param unknown $collection_name
	 * @param unknown $condition
	 * @param unknown $result_condition
	 * @param unknown $fields
	 */
	public function find($server_index, $collection_name, $query_condition, $result_condition=array(), $fields=array()) {
		if (!$this->checkDBEnable($server_index)) return false;
		$result = array();

		try {
			$cursor = $this->getDB($server_index)->$collection_name->find($query_condition, $fields);
			if (isset($result_condition['start'])) $cursor->skip(intval($result_condition['start']));
			if (isset($result_condition['limit']) && $result_condition['limit']) $cursor->limit(intval($result_condition['limit']));
			if (isset($result_condition['sort'])) $cursor->sort($result_condition['sort']);

			while ($cursor->hasNext()) $result[] = $cursor->getNext();
		} catch (MongoConnectionException $e) {
			alert("MongoCursorException: ".$e->getMessage());
			error_log($e->getMessage());
			return false;
		}
		return $result;
	}

	
	/**
	 * 单条查询
	 * @param unknown $collection_name
	 * @param unknown $condition
	 * @param unknown $fields
	 */
	public function findOne($server_index, $collection_name, $condition, $fields=array()) {
		if (!$this->checkDBEnable($server_index)) return false;
		return $this->getDB($server_index)->$collection_name->findOne($condition, $fields);
	}


	/**
	 *
	 * @param array 	$location			array('经度'=>'', '纬度'=> '') 保证顺序!
	 * @param string 	$distance_field
	 * @param float 	$max_distance		最远距离(单位:公里)
	 * @param number 	$start
	 * @param number 	$limit				限制100条
	 */
	public function aggregate($server_index, $collection_name, $location, $match='', $start=0, $limit=25,
							$max_distance=1, $distance_field='gps', $sort='', $dir='') {
		if (!$this->checkDBEnable($server_index)) return false;
		$opt = array();
		$geo = array(
				'$geoNear'=> array(
					'near'				=> $location,
					'distanceField' 	=> $distance_field,
					'distanceMultiplier'=> 6371,
					'spherical'			=> true,
					'num'				=> 1000
			)
		);

		if ($max_distance) $geo['$geoNear']['maxDistance'] = $max_distance/EARTH_RADIUS;
		if ($match) $geo['$geoNear']['query'] = $match;
		$opt[] = $geo;
		if ($sort) {
			if (is_array($sort)) {
				$opt[] = array('$sort'=> $sort);
			} else {
				$opt[] = array('$sort'=> array($sort => ($dir > 0 ? 1 : -1)));
			}
		}
		if ($start) $opt[] = array('$skip'=> intval($start));
		if ($limit) $opt[] = array('$limit' => intval($limit));

		$cl = $this->getDB($server_index)->selectCollection($collection_name);
		$result = $cl->aggregate($opt);

		if ($result['ok'] == 0) error_log($result['errmsg']);

		return $result['ok'] == 1 ? $result['result'] : '';
	}
    
    public function aggregateByOpt($server_index, $collection_name, $opt = array()) {
        $result = $this->getDB($server_index)->selectCollection($collection_name)->aggregate($opt);
        return $result;
    }

	public function group($serverIndex, $collection_name, $key, $initial, $reduce) {
		$result = $this->getDB($serverIndex)
						->selectCollection($collection_name)
						->group(array($key=>1), $initial, $reduce);
		
		return $result;
	}

	public function mapReduce($serverIndex, $collection_name, $map, $reduce) {
		$command = array(
			'mapreduce' => $collection_name,
			'map'		=> $map,
			'reduce'	=> $reduce,
			'out'		=> array('inline'=> 1),
			'verbose'	=> true
		);
		$command_info = $this->command($serverIndex, $command);
		if ($command_info['ok'] == 0) return array();
		return $command_info['results'];
	}
	
	/**
	 * 命令查询
	 * @param array $command
	 * @return array
	 */
	public function command($server_index, $command) {
		if (!$this->checkDBEnable($server_index)) return false;
		return $this->getDB($server_index)->command($command);
	}

	/**
	 * 获取表数据总量
	 * @param string	$collection_name
	 * @return int
	 */
	public function count($server_index, $collection_name, $query_condition = array()) {
		if (!$this->checkDBEnable($server_index)) return false;
		return $this->getDB($server_index)->$collection_name->count($query_condition);
	}
    
    public function listDBs($server_index) {
        return self::$mongoSource[$server_index]->listDBs();
    }
    
    public function listCollections($server_index) {
        $collection_objs = $this->getDB($server_index)->listCollections();
        $collections = array();
        if($collection_objs) {
            foreach($collection_objs as $c) {
                $collections[] = $c->getName();
            }
        }
        return $collections;
    }

	public function __destruct() {
		if (self::$CURRENT_DB) {
			foreach (self::$CURRENT_DB AS $db) {
				if (is_resource($db)) $db->close();
			}
		}
	}
}

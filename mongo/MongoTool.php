<?php
require_once 'MongoBase.php';
class MongoTool{

	protected $serverIndex = '';
	protected $mongoDB = '';
	
	public function __construct() {
		if (!$this->mongoDB) $this->mongoDB = new MongoBase();
	}

	public function setCurrentDB($serverIndex, $db) {
		$this->mongoDB->selectDB($serverIndex, $db);
		$this->serverIndex = $serverIndex;
		return $this;
	}
	
	public function connect($server_index) {
		$this->serverIndex = $server_index;
		return $this->mongoDB->connect($server_index);
	}

	public function ensureIndex($collection_name, $index, $params=array('background'=> true)) {
		return $this->mongoDB->ensureIndex($this->serverIndex, $collection_name, $index, $params);
	}
	
	public function dropIndex($collection_name, $index) {
		return $this->mongoDB->dropIndex($this->serverIndex, $collection_name, $index);
	}

	public function mapReduce($collection_name, $map, $reduce) {
		return $this->mongoDB->mapReduce($this->serverIndex, $collection_name, $map, $reduce);
	}
	
	public function searchIndex($collection_name) {
		return $this->mongoDB->searchIndex($this->serverIndex, $collection_name);
	}
	
	public function insert($collection_name, $data) {
		return $this->mongoDB->insert($this->serverIndex, $collection_name, $data);
	}
	
	public function save($collection_name, $data, $options=array()) {
		return $this->mongoDB->save($this->serverIndex, $collection_name, $data, $options);
	}
	
	public function batchInsert($collection_name, $data, $options=array()) {
		return $this->mongoDB->batchInsert($this->serverIndex, $collection_name, $data, $options);
	}
	
	public function group($collection_name, $key, $initial, $reduce) {
		return $this->mongoDB->group($this->serverIndex, $collection_name, $key, $initial, $reduce);
	}
	
	public function delete($collection_name, $condition, $options=array('justOne'=> true)) {
		return $this->mongoDB->delete($this->serverIndex, $collection_name, $condition, $options);
	}
	
	public function update($colletion_name, $condition, $new_data, $options=array('multiple'=> false)) {
		return $this->mongoDB->update($this->serverIndex, $colletion_name, $condition, $new_data, $options);
	}
	
	public function dataIncrease($colletion_name, $condition, $increase) {
		return $this->mongoDB->dataIncrease($this->serverIndex, $colletion_name, $condition, $increase);
	}
	
	public function unsetFields($colletion_name, $condition, $unset_fields) {
		return $this->mongoDB->unsetFields($this->serverIndex, $colletion_name, $condition, $unset_fields);
	}

	public function find($collection_name, $query_condition, $result_condition=array(), $fields=array()) {
		return $this->mongoDB->find($this->serverIndex, $collection_name, $query_condition, $result_condition, $fields);
	}

	public function findOne($collection_name, $condition, $fields=array()) {
		return $this->mongoDB->findOne($this->serverIndex, $collection_name, $condition);
	}

	public function aggregate($collection_name, $location, $match='', $start=0, $limit=25, $max_distance=0.008, $distance_field='gps', $order_by='', $sort='') {
		return $this->mongoDB->aggregate($this->serverIndex, $collection_name, $location, $match, $start, $limit, $max_distance, $distance_field, $order_by, $sort);
	}
    
    public function aggregateByOpt($collection_name, $opt) {
        return $this->mongoDB->aggregateByOpt($this->serverIndex, $collection_name, $opt);
    }
    
	public function command($command) {
		return $this->mongoDB->command($this->serverIndex, $command);
	}

	public function count($collection_name, $query_condition = array()) {
		return $this->mongoDB->count($this->serverIndex, $collection_name, $query_condition);
	}
    
    public function listDBs() {
        return $this->mongoDB->listDBs($this->serverIndex);
    }
    
    public function listCollections() {
        return $this->mongoDB->listCollections($this->serverIndex);
    }
    
}

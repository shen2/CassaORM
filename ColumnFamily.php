<?php
namespace CassaORM;

class ColumnFamily extends \phpcassa\ColumnFamily{
	
	/**
	 *
	 * @var ConnectionPool
	 */
	protected static $_connectionPool;
	
	protected $_rowClass = 'CassaORM\Model';
	
	public static function setConnectionPool($connectionPool)
	{
		self::$_connectionPool = $connectionPool;
	}
	
	/**
	 * 
	 * @param string $rowClass
	 * @return ColumnFamily
	 */
	public function setRowClass($rowClass){
		$this->_rowClass = $rowClass;
		return $this;
	}
	
	/**
	 * @return Model
	 */
	public function create($key, $data){
		$row = new $this->_rowClass($data, $this);
		$row->key = $key;
		return $row;
	}
	
	/**
	 * 
	 * @see \phpcassa\ColumnFamily::_get()
	 * @return Model
	 */
	protected function _get($key, $cp, $slice, $cl) {
		$resp = $this->pool->call("get_slice",
				$this->pack_key($key),
				$cp, $slice, $this->rcl($cl));
	
		if (count($resp) == 0)
			throw new \cassandra\NotFoundException();
	
		$row = $this->unpack_coscs($resp);
		$row->key = $key;
		
		return $row;
	}
	
	/**
	 * 
	 * @see \phpcassa\ColumnFamily::_multiget()
	 */
	protected function _multiget($keys, $cp, $slice, $cl, $buffsz) {
		$ret = parent::_multiget($keys, $cp, $slice, $cl, $buffsz);
		
		foreach($ret as $key => $row)
			$row->key = $key;
		
		return $ret;
	}
	
	/**
	 * 
	 * @param array $array_of_coscs
	 * @return \CassaORM\Model
	 */
	protected function unpack_coscs($array_of_coscs) {
		$data = array();
		$timestamp = array();
		$ttl = array();
		
		$first = $array_of_coscs[0];
		if($first->column) { // normal columns
			foreach($array_of_coscs as $cosc) {
				$name = $this->unpack_name($cosc->column->name, false);
				$data[$name] = $this->unpack_value($cosc->column->value, $cosc->column->name);
				$timestamp[$name] = $cosc->column->timestamp;
				$ttl[$name] = $cosc->column->ttl;
			}
		}
		else if ($first->counter_column) {
			foreach($array_of_coscs as $cosc) {
				$name = $this->unpack_name($cosc->counter_column->name, false);
				$data[$name] = $cosc->counter_column->value;
				$timestamp[$name] = $cosc->counter_column->timestamp;
				$ttl[$name] = $cosc->counter_column->ttl;
			}
		}
		
		return new $this->_rowClass($data, $this, $timestamp, $ttl);
	}
}

<?php
namespace CassaORM;

class Model extends \ArrayObject
{
	/**
	 * @var ColumnFamily
	 */
	protected $_columnFamily;
	
	/**
	 * Tracks columns where data has been updated. Allows more specific insert and
	 * update operations.
	 *
	 * @var array
	 */
	protected $_modifiedData = array();
	
	/**
	 * timestamp
	 *
	 * @var array
	 */
	public $timestamp = array();
	
	/**
	 * ttl
	 * @var array
	 */
	public $ttl = array();
	
	public $key;
	
	/**
	 * 构造函数
	 * @param array $data
	 * @param ColumnFamily $columnFamily
	 * @param array $timestamp
	 * @param array $ttl
	 */
	public function __construct($data, $columnFamily, $timestamp = array(), $ttl = array()){
		parent::__construct($data);
		
		$this->_columnFamily = $columnFamily;
		$this->timestamp = $timestamp;
		$this->ttl = $ttl;
		
		if (empty($timestamp)) {
			$this->_modifiedData = $this->getArrayCopy();
		}
		
		//$this->init();
	}
	
	public function offsetGet($columnName)
	{
		return parent::offsetExists($columnName) ? parent::offsetGet($columnName) : null;
	}
	
	/**
	 * Set row field value
	 *
	 * @param  string $columnName The column key.
	 * @param  mixed  $value      The value for the property.
	 * @return void
	 */
	public function offsetSet($columnName, $value)
	{
		parent::offsetSet($columnName,$value);
		$this->_modifiedData[$columnName] = $value;
	}
	
	public function offsetUnset($columnName)
	{
		parent::offsetUnset($columnName);
		$this->_modifiedData[$columnName] = null;
	}
	
	public function save()
	{
		if (!isset($this->_columnFamily))
			$this->_columnFamily = new ColumnFamily(self::$_connectionPool, static::$_name);
		
		$removedColumns = array_keys($this->_modifiedData, null);
		if (!empty($removedColumns)){
			foreach($removedColumns as $column)
				unset($this->_modifiedData[$column]);
			$this->_columnFamily->remove($this->key, $removedColumns);
		}
		
		if (!empty($this->_modifiedData)){
			$this->_columnFamily->insert($this->key, $this->_modifiedData);
			$this->_modifiedData = array();
		}
		
		return $this;
	}
	
	/**
	 * 删除一整行
	 * @return \CassaORM\Model
	 */
	public function remove(){
		if (!isset($this->_columnFamily))
			$this->_columnFamily = new ColumnFamily(self::$_connectionPool, static::$_name);
		
		$this->_columnFamily->remove($this->key);
		
		return $this;
	}
}

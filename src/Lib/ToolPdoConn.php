<?php
/**
 * Created by PhpStorm.
 * User: javion
 */

namespace Javion\cu_queue\Lib;
use Medoo\Medoo;

class ToolPdoConn
{
    private $strDb;
    private $strHost;
    private $intPort;
    private $strTable;
    private $strUserName;
    private $strPwd;
    private $isInTrans = false;
    /**
     * @var $db Medoo
     */
    private $db = null;

    public function __construct($strDb, $strHost, $strUserName, $strPwd, $intPort = 3306)
    {
        $this->strDb = $strDb;
        $this->strHost = $strHost;
        $this->strUserName = $strUserName;
        $this->strPwd = $strPwd;
        $this->intPort = $intPort;
        $this->connect();
    }

    private function connect(){
        $this->db = new Medoo([
            'database_type' => 'mysql',
            'database_name' => $this->strDb,
            'server' => $this->strHost,
            'port' => (int)$this->intPort,
            'username' => $this->strUserName,
            'password' => $this->strPwd
        ]);
    }

    public function setTableName($strTable){
        $this->strTable = $strTable;
    }

    public function insertRecord($arrInsert){
        $this->callPdoExec('insert', array($this->strTable, $arrInsert));

        return $this->db->id();
    }

    public function addAll($arrInsert){
        $this->callPdoExec('insert', array($this->strTable, $arrInsert));

        return true;
    }

    public function getListByConds($arrField, $arrCond){
        $result = $this->callPdoExec('select', array($this->strTable, $arrField, $arrCond));

        return $result;
    }

    public function updateByConds($arrField, $arrCond) {
        return $this->callPdoExec('update', array($this->strTable, $arrField, $arrCond));
    }

    public function getRecordByConds($arrField, $arrCond){
        return $this->callPdoExec('get', array($this->strTable, $arrField, $arrCond));
    }

    public function deleteByConds($arrCond){
        return $this->callPdoExec('delete', array($this->strTable, $arrCond));
    }

    public function getCntByConds($arrCond){
        return $this->callPdoExec('count', array($this->strTable, $arrCond));
    }

    public function startTransaction(){
        $this->isInTrans = true;
        $this->db->pdo->beginTransaction();
    }

    public function commit(){
        if ($this->db->pdo->inTransaction()){
            $this->db->pdo->commit();
        }
    }

    public function rollback(){
        if ($this->db->pdo->inTransaction()){
            $this->db->pdo->rollBack();
        }
    }
    /**
     * 真正数据库操作执行
     *
     * @param $strMethod
     * @param $arrParam
     * @return mixed
     */
    private function callPdoExec($strMethod, $arrParam){
        try{
            return call_user_func_array(array($this->db,$strMethod),$arrParam);
        }catch (\PDOException $e){
            if (in_array($e->errorInfo[1], array(2006,2013)) && !$this->isInTrans){//非事务操作 断线重连
                $this->connect();
                $this->callPdoExec($strMethod, $arrParam);
            }else{
                throw $e;
            }
        }

        return false;
    }

    /**
     * @throws \Exception
     */
    public function __clone()
    {
        throw new \Exception("can not clone");
    }

}
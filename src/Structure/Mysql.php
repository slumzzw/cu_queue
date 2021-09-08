<?php
/**
 * mysql structure class
 * User: javion
 */
namespace Javion\cu_queue\Structure;

use Javion\cu_queue\Lib\ToolArray;
use Javion\cu_queue\Lib\ToolPdoConn;
use Javion\cu_queue\Log;
use Javion\cu_queue\Task;

class Mysql implements StructureInterface {
    private $strHost = null; //数据库链接host
    private $intPort = 3306; //数据库服务端口，默认3306
    private $strUserName = null; //数据库用户
    private $strPwd = null; //数据库密码
    private $strDbName = null; //队列数据库名
    private $strTableName = null; //队列表名
    private $strDelayTableName = null; //延时队列记录表
    /**
     * @var $objLog Log
     */
    private $objLog = null;
    private $arrProcessConf = null;
    /**
     * 数据库链接dao
     * @var $objDaoQueue ToolPdoConn
     */
    private $objDaoQueue = null;

    public function initDeal()
    {
        $this->objDaoQueue = null;
    }

    /**
     * @param array $options
     * @return mixed|void
     * @throws \Exception
     */
    public function setOptions(array $options)
    {
        $baseConfig = $options['base'];
        if (!isset($baseConfig['db_connect_name']) || empty($baseConfig['db_connect_name'])) {
            $this->objLog->log('db_connect_name is empty');
            throw new \Exception('db_connect_name 配置不能为空');
        }
        if (!isset($baseConfig['db_table_name']) || empty($baseConfig['db_table_name'])) {
            $this->objLog->log('db_table_name is empty');
            throw new \Exception('db_table_name 配置不能为空');
        }
        if (!isset($baseConfig['db_delay_table_name']) || empty($baseConfig['db_delay_table_name'])) {
            $this->objLog->log('db_delay_table_name is empty');
            throw new \Exception('db_delay_table_name 配置不能为空');
        }
        if (empty($baseConfig['db_host'])) {
            $this->objLog->log('db_host is empty');
            throw new \Exception('db_host 配置不能为空');
        }
        if (empty($baseConfig['db_user'])) {
            $this->objLog->log('db_user is empty');
            throw new \Exception('db_user 配置不能为空');
        }
        if (empty($baseConfig['db_pwd'])) {
            $this->objLog->log('db_pwd is empty');
            throw new \Exception('db_pwd 配置不能为空');
        }
        $this->strDbName = $baseConfig['db_connect_name'];
        $this->strTableName = $baseConfig['db_table_name'];
        $this->strDelayTableName = $baseConfig['db_delay_table_name'];
        $this->strHost = $baseConfig['db_host'];
        $this->strUserName = $baseConfig['db_user'];
        $this->strPwd = $baseConfig['db_pwd'];
        !empty($baseConfig['db_port']) && $this->intPort = $baseConfig['db_port'];
        $this->arrProcessConf = $options['process'];
    }

    public function getStructureName(){
        return 'mysql';
    }

    /**
     * @return mixed
     */
    public function getOptions()
    {
       return;
    }

    public function addTask(Task $task)
    {
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        if ($task->getPreexecTime() <= time()){
            $arrInsert = array(
                'type' => $task->getType(),
                'sub_type' => $task->getSubType(),
                'param_content' => $task->getParamContent(),
                'create_time' => time(),
                'preexec_time' => $task->getPreexecTime(),
                'p_key' => $task->getPkey(),
                'level' => $task->getLevel()
            );
            $result = $objDaoQueue->insertRecord($arrInsert);
        }else{
            $arrDelayInsert = array(
                'type' => $task->getType(),
                'sub_type' => $task->getSubType(),
                'param_content' => $task->getParamContent(),
                'create_time' => time(),
                'preexec_time' => $task->getPreexecTime(),
                'p_key' => $task->getPkey(),
                'level' => $task->getLevel(),
                'try_num' => 0,
                'relate_old_id' => 0,
                'preexec_day' => date('Ymd', $task->getPreexecTime())
            );
            $result = $this->addDelayQueue(array($arrDelayInsert));
        }

        return $result;
    }

    public function addTaskList($arrTaskList)
    {
        $result = 0;
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        $arrInsert = $arrDelayQueue = array();
        foreach ($arrTaskList as $task){
            /**
             * @var $task Task
             */
            if ($task->getPreexecTime() <= time()){
                $arrInsert[] = array(
                    'type' => $task->getType(),
                    'sub_type' => $task->getSubType(),
                    'param_content' => $task->getParamContent(),
                    'create_time' => time(),
                    'preexec_time' => $task->getPreexecTime(),
                    'p_key' => $task->getPkey(),
                    'level' => $task->getLevel()
                );
            }else{
                $arrDelayQueue[] = array(
                    'type' => $task->getType(),
                    'sub_type' => $task->getSubType(),
                    'param_content' => $task->getParamContent(),
                    'create_time' => time(),
                    'preexec_time' => $task->getPreexecTime(),
                    'p_key' => $task->getPkey(),
                    'level' => $task->getLevel(),
                    'try_num' => 0,
                    'relate_old_id' => 0,
                    'preexec_day' => date('Ymd', $task->getPreexecTime())
                );
            }

        }
        if ($arrInsert){
            $result = $objDaoQueue->addAll($arrInsert);
        }
        if ($arrDelayQueue){
            $this->addDelayQueue($arrDelayQueue);
        }

        return $result;
    }

    public function getTasks($type, $intNum, $preexec_time = null)
    {
        $pid = posix_getpid();
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        //获取未消费的队列
        $arrConds = array(
            "AND" => array(
                "conn_id" => 0,
                "type" => intval($type),
                "status" => Task::STATUS_NEW,
            ),
            'LIMIT' => $intNum
        );

        $arrQueueList = $objDaoQueue->getListByConds('*', $arrConds);
        $arrIds = ToolArray::getFieldValue($arrQueueList, 'id');
        if (empty($arrIds)){
            return array();
        }
        $arrCond = array(
            "id" => $arrIds
        );
        $result = $objDaoQueue->updateByConds(['conn_id' => $pid, 'update_time' => time(), 'status' => Task::STATUS_PROCESS], $arrCond);
        if (empty($result)){
            return [];
        }
        $arrCond = array(
            'conn_id' => $pid,
            'status' => Task::STATUS_PROCESS,
        );
        $arrQueueList = $objDaoQueue->getListByConds('*', $arrCond);
        !$arrQueueList && $arrQueueList = [];

        return $arrQueueList;
    }

    public function updateTask($status, $mark, $id, $arrExt = array()){
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        $dbQueueLog = $objDaoQueue->getRecordByConds('*', array('id' => $id));
        $arrdata = array(
            "status" => $status,
            'mark' => $mark,
            'update_time' => time()
        );
        if (Task::STATUS_NEW == $status){
            $arrdata['conn_id'] = 0;
            $arrdata['status'] = Task::STATUS_SUCCESS;
            $dbQueueLog['preexec_time'] = time();
            isset($arrExt['preexec_time']) && $dbQueueLog['preexec_time'] = $arrExt['preexec_time'];
            isset($arrExt['try_num']) && $dbQueueLog['try_num'] = $arrExt['try_num'];
            isset($arrExt['type']) && $dbQueueLog['type'] = $arrExt['type'];
            isset($arrExt['sub_type']) && $dbQueueLog['sub_type'] = $arrExt['sub_type'];
            $dbQueueLog['relate_old_id'] = $id;
            $this->addDelayQueue(array($dbQueueLog));
        }
        $objDaoQueue->setTableName($this->strTableName);
        $objDaoQueue->updateByConds($arrdata, ['id' => $id]);
    }

    /**
     * 获取消费率
     * @param $arrPid
     * @param $arrQueueType
     * @return array
     *
     */
    public function getConsumeRatio($arrPid, $arrQueueType)
    {
        //延时队列检测
        $statrTime = time() - 60*60*24*7; //一周前
        $endTime = time();
        $arrCond = array(
            'AND' => array(
                'preexec_time[<>]' => array((int)$statrTime, (int)$endTime),
            ),
            'LIMIT' => 3000,
        );
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strDelayTableName);
        $arrDelayData = $objDaoQueue->getListByConds('*', $arrCond);
        if ($arrDelayData){
            $arrDelayData = array_chunk($arrDelayData, 500);
            foreach ($arrDelayData as $item){
                $arrQueueLog = $arrDelayId = array();
                foreach ($item as $value){
                    $arrDelayId[] = $value['id'];
                    unset($value['id'], $value['update_time'], $value['relate_old_id'], $value['preexec_day'], $value['is_exec']);
                    $arrQueueLog[] = $value;
                }
                $objDaoQueue->startTransaction();
                try{
                    //入表 删延时表/修改状态
                    $objDaoQueue->setTableName($this->strTableName);
                    $objDaoQueue->addAll($arrQueueLog);
                    $objDaoQueue->setTableName($this->strDelayTableName);
                    $objDaoQueue->deleteByConds(array("id" => $arrDelayId));
                    $objDaoQueue->commit();
                    $this->objLog->log('deal delay num', array('num' => count($arrQueueLog)));
                }catch (\PDOException $e) {
                    $objDaoQueue->rollback();
                }
            }
        }
        //处理数据
        $result = array();
        $objDaoQueue->setTableName($this->strTableName);
        //获取未消费的队列 计算消费比
        foreach ($arrQueueType as $item){
            $arrConds = array(
                'type' => $item,
                'conn_id' => 0,
                "status" => 0,
            );
            $leftNum = $objDaoQueue->getCntByConds($arrConds);
            $thresholdNum = $this->arrProcessConf[$item]['thresholdNum'];
            $leftNum = $leftNum ? $leftNum : 0;
            $ratio = round($leftNum/$thresholdNum, 2);
            $result[$item] = array(
                'type' => $item,
                'ratio' => $ratio,
                'leftNum' => $leftNum
            );
        }

        return $result;
    }

    /**
     * 释放超时的队列
     *
     * @param $arrPid
     * @param $type
     * @param $arrTypeSubMap
     * @return bool
     */
    public function resetHoldMess($arrPid, $type, $arrTypeSubMap) {
        if(empty($arrPid)) {
            return true;
        }
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        $arrConds = array(
            "type" => $type,
            "status" => Task::STATUS_PROCESS,
            "conn_id[!]" => $arrPid,
            'update_time[<=]' => time()-3600, //1h前
        );
        $arrList = $objDaoQueue->getListByConds(array('id', 'type', 'sub_type', 'try_num'), $arrConds);
        if(empty($arrList)) {
            return true;
        }
        //判断重试次数，超过次数修改为失败
        $arrReTry = $arrFail = array();
        foreach ($arrList as $item){
            if (isset($arrTypeSubMap[$item['type']][$item['sub_type']])){
                if ($arrTypeSubMap[$item['type']][$item['sub_type']]['tryNum'] <= $item['try_num']){
                    $arrFail[] = $item['id'];
                }else{
                    $arrReTry[] = $item['id'];
                }
            }else{
                $arrFail[] = $item['id'];
            }
        }
        if ($arrReTry){
            $arrReConds = $arrConds;
            $arrReConds["id"] = $arrReTry;
            $arrSave = array(
                'conn_id' => 0,
                'status' => Task::STATUS_NEW,
                'update_time' => time(),
                'preexec_time' => time() + 60,
                'mark' => '重置',
                'try_num[+]' => 1,
            );
            $objDaoQueue->updateByConds($arrSave, $arrReConds);
        }

        if ($arrFail){
            $arrFaConds = $arrConds;
            $arrFaConds["id"] = $arrFail;
            $arrSave = array(
                'status' => Task::STATUS_FAIL,
                'update_time' => time(),
                'mark' => '达到重置最大次数',
            );
            $objDaoQueue->updateByConds($arrSave, $arrFaConds);
        }

        return true;
    }

    /**
     * 平滑退出复位未消费状态
     *
     * @param $type
     * @return bool
     */
    public function quitDeal($type) {
        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strTableName);
        $intConnId = posix_getpid();
        $arrConds = array(
            "type" => $type,
            "status" => Task::STATUS_PROCESS,
            "conn_id" => $intConnId,
        );
        $arrList = $objDaoQueue->getListByConds(array('id'), $arrConds);
        if(empty($arrList)) {
            return true;
        }
        $arrId = ToolArray::getFieldValue($arrList, 'id');
        $arrConds["id"] = $arrId;
        $arrSave = array(
            'conn_id' => 0,
            'status' => Task::STATUS_NEW,
            'update_time' => time(),
            'mark' => '退出复位'
        );
        $objDaoQueue->updateByConds($arrSave, $arrConds);

        return true;
    }

    /**
     * 判断任务是否存在，且未消费
     * @param Task $task
     * @return mixed
     */
    public function hasTask(Task $task)
    {
        $objDaoQueue = $this->getMyCont();
        //队列延时表
        $objDaoQueue->setTableName($this->strDelayTableName);
        $arrConds = array(
            'type' => $task->getType(),
            'sub_type' => $task->getSubType(),
            'p_key' => $task->getPkey(),
            'is_exec' => 0,
            'preexec_time[>=]' => time()+5*60,
        );
        $res2 = $objDaoQueue->getCntByConds($arrConds);

        return $res2;
    }

    /**
     * 加入延时队列
     * @param $arrQueueData
     * @return bool
     */
    private function addDelayQueue($arrQueueData){
        if (empty($arrQueueData)){
            return false;
        }
        $arrInsert = array();
        foreach ($arrQueueData as $item){
            $arrInsert[] = array(
                'type' => $item['type'],
                'sub_type' => $item['sub_type'],
                'param_content' => $item['param_content'],
                'create_time' => time(),
                'preexec_time' => $item['preexec_time'],
                'p_key' => $item['p_key'],
                'level' => $item['level'],
                'try_num' => $item['try_num'],
                'relate_old_id' => $item['relate_old_id'],
                'preexec_day' => date('Ymd', $item['preexec_time'])
            );
        }

        $objDaoQueue = $this->getMyCont();
        $objDaoQueue->setTableName($this->strDelayTableName);
        return $objDaoQueue->addAll($arrInsert);
    }

    /**
     * @param Log $log
     */
    public function setLogObj(Log $log)
    {
        $this->objLog = $log;
    }

    /**
     * 获取数据库链接
     *
     * @return ToolPdoConn|null
     */
    private function getMyCont(){
        if ($this->objDaoQueue == null){
            $this->objDaoQueue = new ToolPdoConn($this->strDbName, $this->strHost, $this->strUserName, $this->strPwd, $this->intPort);
            $this->objDaoQueue->setTableName($this->strTableName);
        }

        return $this->objDaoQueue;
    }

    /* 队列表结构
  CREATE TABLE `common_queue` (
	`id` BIGINT ( 11 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`sub_type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`conn_id` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '消费者标识',
	`param_content` text COMMENT '队列入参',
	`status` TINYINT ( 2 ) NOT NULL DEFAULT '0' COMMENT '0新建 1消费中 2成功 3失败 4需重试',
	`create_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '创建时间',
	`update_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '状态变更时间',
	`preexec_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费时间',
	`p_key` VARCHAR ( 100 ) NOT NULL DEFAULT '' COMMENT '业务唯一标识key，查询用',
	`mark` VARCHAR ( 255 ) NOT NULL DEFAULT '' COMMENT '备注',
	`level` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '优先级 值越大优先级越高',
	`try_num` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '重试次数',
	PRIMARY KEY ( `id` ),
	KEY `indx_s` ( `p_key`, `type` ) USING BTREE,
	KEY `indx_exec` ( `conn_id`, `status` ) USING BTREE,
	KEY `indx_level` ( `level` ) USING BTREE,
	KEY `indx_type_status` ( `type`, `status` ),
	KEY `indx_status` ( `status` ),
    KEY `indx_get` ( `conn_id`, `type`, `status`, `preexec_time` ) USING BTREE
    ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4;

    CREATE TABLE `common_delay_queue` (
	`id` BIGINT ( 20 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`sub_type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`param_content` text COMMENT '队列入参',
	`create_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '创建时间',
	`update_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '状态变更时间',
	`preexec_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费时间',
	`p_key` VARCHAR ( 100 ) NOT NULL DEFAULT '' COMMENT '业务唯一标识key，查询用',
	`level` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '优先级 值越大优先级越高',
	`try_num` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '重试次数',
	`relate_old_id` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '关联的旧id',
	`preexec_day` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费日期',
	`is_exec` TINYINT ( 1 ) NOT NULL DEFAULT '0' COMMENT '是否执行 0 否 1执行',
	PRIMARY KEY ( `id` ),
	KEY `indx_day` ( `preexec_time` ) USING BTREE,
	KEY `indx_day_time` ( `preexec_day`, `preexec_time` ) USING BTREE,
	KEY `indx_key` ( `p_key` ) USING BTREE,
	KEY `indx_type` ( `type`, `sub_type` ) USING BTREE
    ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4
     */
}
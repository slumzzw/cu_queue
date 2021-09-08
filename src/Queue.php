<?php
/**
 * queue
 * User: javion
 */
namespace Javion\cu_queue;

use Javion\cu_queue\Lib\ToolArray;
use Javion\cu_queue\Structure\StructureInterface;

Class Queue{
    const MYSQL_QUEUE = 'mysql';
    const RABBITMQ_QUEUE = 'rabbitmq';

    private $isInit = false;
    //-----------------------------配置参数---------------------------------//
    //队列配置文件
    private $configPath = '';
    //日志文件
    public $logFile = '';
    //master主进程运行时间 秒
    public $masterExecTime = 43200;
    //worker子进程运行时间 秒
    public $wokerExecTime = 2400;
    //脚本前缀名
    public $scriptName = 'common_used_queue';
    //master进程名字
    public $masterName = 'master';
    //woker进程名字
    public $workerName = 'worker';
    //timer进程名称
    public $timerName = 'timer';
    //管道文件目录
    public $pipePath = '';
    //配置数组信息
    public $arrConfig = array();
    //队列信息
    public $arrProcess = array();
    //subtype信息
    public $arrSubtype = array();
    //subtype Map 数据
    public $arrTypeSubMap = array();

    //-----------------------------注入对象---------------------------------//
    //日志对象
    private $objLog = null;
    //队列载体对象
    private $objStructure = null;
    //钉钉通知webhook
    public $strDingHook = null;

    public function __construct($configPath = '')
    {
        $this->configPath = $configPath ? $configPath : dirname(__FILE__) . '/config.ini';
        $this->logFile = dirname(__FILE__).'/commonQueue.log';
        $this->objLog = new Log($this->logFile);
        self::init();
    }

    /**
     * 设置配置文件绝对路径 错误路径返回false
     *
     * @param $configPath
     * @return bool
     */
    public function setConfigPath ($configPath){
        if (is_file($configPath)){
            $this->configPath = $configPath;
            $this->isInit = false;
            return true;
        }

        return false;
    }

    public function getConfigPath(){
        return $this->configPath;
    }

    /**
     * 初始化、检测配置信息
     * @throws \Exception
     */
    private function init(){
        if ($this->isInit){
            return;
        }
        //支持php5.6及以上版本
        if (version_compare(PHP_VERSION, '5.6.0') == -1){
            $this->log('php version need ge 5.6.0 ');
            throw new \Exception('php version need ge 5.6.0 ');
        }
        $arrNeedCheck = array(
            'base' => array('queue_type', 'structure', 'pipe_path')
        );
        $arrConf = array();
        if (file_exists($this->configPath)){
            $arrConf = $this->parseIniFile($this->configPath);
        }
        if (empty($arrConf)){
            $this->log('config error', array('confFile' => $this->configPath));
            die('config error');
        }
        //检查必要参数
        foreach ($arrNeedCheck as $section => $arrItem){
            if (!isset($arrConf[$section])){
                $this->log('config error: has no base config', array('confFile' => $this->configPath));
                die('config error: has no base config');
            }
            foreach ($arrItem as $item){
                if (!isset($arrConf[$section][$item]) || empty($arrConf[$section][$item])){
                    $this->log("config error: base config $item error", array('confFile' => $this->configPath, 'item' => $item));
                    die("config error: base config $item error");
                }
            }
        }
        //base 参数赋值
        $this->pipePath = $arrConf['base']['pipe_path'];
        isset($arrConf['base']['master_exec_time']) && $arrConf['base']['master_exec_time'] && $this->masterExecTime = $arrConf['base']['master_exec_time'];
        isset($arrConf['base']['woker_exec_time']) && $arrConf['base']['woker_exec_time'] && $this->wokerExecTime = $arrConf['base']['woker_exec_time'];
        isset($arrConf['base']['script_name']) && $arrConf['base']['script_name'] && $this->scriptName = $arrConf['base']['script_name'];
        isset($arrConf['base']['master_name']) && $arrConf['base']['master_name'] && $this->masterName = $arrConf['base']['master_name'];
        isset($arrConf['base']['worker_name']) && $arrConf['base']['worker_name'] && $this->workerName = $arrConf['base']['worker_name'];
        if (isset($arrConf['base']['log_file']) && $arrConf['base']['log_file']){
            $this->logFile = $arrConf['base']['log_file'];
            $this->objLog->setLogFile($this->logFile);
        }
        if (!empty($arrConf['base']['ding_hook'])){
            $this->strDingHook = $arrConf['base']['ding_hook'];
        }
        //队列类型数据
        $arrayType = array();
        $arrQueueNeedCheck = array('type');
        $arrQueue = $arrConf['base']['queue_type'];
        foreach ($arrQueue as $item){
            if (!isset($arrConf[$item])){
                $this->log("config error: queue '$item' config error", array('confFile' => $this->configPath, 'item' => $item));
                die("config error: queue '$item' config error");
            }
            foreach ($arrQueueNeedCheck as $v){
                if (!isset($arrConf[$item][$v]) || !is_numeric($arrConf[$item][$v]) || in_array($arrConf[$item][$v], $arrayType)){
                    $this->log("config error: please check queue '$item' '$v' config", array('confFile' => $this->configPath, 'item' => $v));
                    die("config error: please check queue '$item' '$v' config");
                }
                $arrayType[] = $arrConf[$item][$v];
            }
            $queue = array(
                'type' => $arrConf[$item]['type'],
                'maxProcessNum' => isset($arrConf[$item]['max_process_num']) ? $arrConf[$item]['max_process_num'] : 10,
                'minProcessNum' => isset($arrConf[$item]['min_process_num']) ? $arrConf[$item]['min_process_num'] : 1,
                'thresholdNum' => isset($arrConf[$item]['threshold_num']) ? $arrConf[$item]['threshold_num'] : 5000,
                'tryNum' => isset($arrConf[$item]['try_num']) ? $arrConf[$item]['try_num'] : 3,
                'isFailNotice' => isset($arrConf[$item]['is_fail_notice']) ? $arrConf[$item]['is_fail_notice'] : 0,
                'isMoreNotice' => isset($arrConf[$item]['is_more_notice']) ? $arrConf[$item]['is_more_notice'] : 0,
                'typeName' => $item,
                'consumeNum' => isset($arrConf[$item]['consume_num']) ? $arrConf[$item]['consume_num'] : 10,
            );
            $this->arrProcess[$arrConf[$item]['type']] = $queue;
        }
        //队列subtype task配置
        $arrTaskNeedCheck = array('sub_type', 'class', 'method');
        $arrayType = array();
        foreach ($arrConf as $key => $value){
            if ($key == 'base' || in_array($key, $arrQueue)){
                continue;
            }
            $arrSubType = explode('_', $key);
            if (count($arrSubType) != 2 || !in_array($arrSubType[0], $arrQueue) || empty($arrSubType[1])){
                continue;
            }
            foreach ($arrTaskNeedCheck as $v){
                if (!isset($arrConf[$key][$v]) || empty($arrConf[$key][$v])){
                    $this->log("config error: please check task '$key' '$v' config", array('confFile' => $this->configPath, 'item' => $v));
                    die("config error: please check task '$key' '$v' config");
                }
                if (!isset($arrayType[$arrConf[$arrSubType[0]]['type']])){
                    $arrayType[$arrConf[$arrSubType[0]]['type']] = array();
                }
                if ($v == 'sub_type' && (!is_numeric($arrConf[$key][$v]) || in_array($arrConf[$key][$v], $arrayType[$arrConf[$arrSubType[0]]['type']]))){
                    $this->log("config error: please check task '$key' '$v' config", array('confFile' => $this->configPath, 'item' => $v));
                    die("config error: please check task '$key' '$v' config");
                }
                $arrayType[$arrConf[$arrSubType[0]]['type']][] = $arrConf[$key][$v];
            }
            $arrSubTypeConf = array(
                'type' => $arrConf[$arrSubType[0]]['type'],
                'subType' => $arrConf[$key]['sub_type'],
                'typeName' => $arrSubType[0],
                'class' => $arrConf[$key]['class'],
                'method' => $arrConf[$key]['method'],
                'tryNum' => isset($arrConf[$key]['try_num']) ? $arrConf[$key]['try_num'] :
                    (isset($arrConf[$arrSubType[0]]['try_num']) ? $arrConf[$arrSubType[0]]['try_num'] : 3),
                'isFailNotice' => isset($arrConf[$key]['is_fail_notice']) ? $arrConf[$key]['is_fail_notice'] :
                    (isset($arrConf[$arrSubType[0]]['is_fail_notice']) ? $arrConf[$arrSubType[0]]['is_fail_notice'] : false),
                'level' => isset($arrConf[$key]['level']) ? $arrConf[$key]['level'] : 0,
                'checkParams' => isset($arrConf[$key]['check_param']) ? $arrConf[$key]['check_param'] : array(),
                'isWriteMysqlLog' => isset($arrConf[$key]['is_write_mysql_log']) ? $arrConf[$key]['is_write_mysql_log'] : true,
            );
            $this->arrSubtype[$key] = $arrSubTypeConf;
            $this->arrTypeSubMap[$arrSubTypeConf['type']][$arrSubTypeConf['subType']] = $arrSubTypeConf;
        }

        $this->arrConfig = $arrConf;
        //队列载体实例化
        if (!isset($this->arrConfig['base']['structure'])){
            $this->log('config error', array('confFile' => $this->configPath));
            die('config error');
        }
        if ($this->arrConfig['base']['structure'] == self::RABBITMQ_QUEUE && !isset($this->arrConfig['base']['rabbit_conf'])){
            $this->log('rabbit config error', array('confFile' => $this->configPath));
            die('config error');
        }
        $structureClass = "\\Javion\\cu_queue\\Structure\\". ucfirst($this->arrConfig['base']['structure']);
        if (class_exists($structureClass)) {
            $structure = new $structureClass;
        } elseif (class_exists($this->arrConfig['base']['structure'])) {
            $structureClass = $this->arrConfig['base']['structure'];
            $structure = new $structureClass;
        }

        if (!isset($structure) || !is_object($structure)) {
            $this->log("structure do not exist", array('structure' => $this->arrConfig['base']['structure']));
            die('structure do not exist');
        }
        if (!($structure instanceof StructureInterface)) {
            $this->log("structure do not implement Glo_Cq_Structure_StructureInterface", array('structure' => $this->arrConfig['base']['structure']));
            die('structure do not implement Glo_Cq_Structure_StructureInterface');
        }
        $arrStructureConfig = array(
            'base' => $this->arrConfig['base'],
            'process' => $this->arrProcess
        );
        $structure->setLogObj($this->objLog);
        $structure->setOptions($arrStructureConfig);
        $this->setStructure($structure);
        $this->isInit = true;
    }

    private function parseIniFile($file){
        $arrConf = parse_ini_file($this->configPath, true);
        foreach ($arrConf as $key => $value){
            if (strpos($key, ':') !== false){
                unset($arrConf[$key]);
                $strNewKey = explode(':', $key)[0];
                $arrConf[$strNewKey] = $value;
            }
        }

        return $arrConf;
    }

    /**
     * 服务开启
     */
    public function startService() {
        if (substr(php_sapi_name(), 0, 3) !== 'cli') {
            die("This Programe can only be run in CLI mode");
        }
        //初始化配置
        self::init();
        //master 进程启动
        $objMaster = new Master($this);
        $objMaster->runProcess();

        return $this;
    }

    /**
     * @return StructureInterface $objStructure
     */
    public function getStructure()
    {
        return $this->objStructure;
    }

    /**
     * @param StructureInterface $objStructure
     * @return $this
     */
    public function setStructure(StructureInterface $objStructure)
    {
        $this->objStructure = $objStructure;

        return $this;
    }

    /**
     * 单个入队列
     *
     * @param $typeName /队列类型标识名
     * @param $params   /消费参数
     * @param string $pKey /数据表查询标识
     * @param int $preExecTime /消费时间戳，0表示当前时间
     * @return bool
     */
    public function addTask($typeName, $params, $pKey = '', $preExecTime = 0)
    {
        $arrTypeConf = $this->arrSubtype[$typeName];
        $isWriteMysqlLog = isset($arrTypeConf['isWriteMysqlLog']) ? $arrTypeConf['isWriteMysqlLog'] : true;
        list($typeName, $subTypeName) = explode('_', $typeName);
        $taskData['type'] = $typeName;
        $taskData['sub_type'] = $subTypeName;
        $taskData['param_content'] = $params;
        $taskData['is_write_mysql_log'] = $isWriteMysqlLog;
        $pKey && $taskData['pKey'] = $pKey;
        $preExecTime && $taskData['preexec_time'] = $preExecTime;
        $objTask = new Task($this);
        $checkResult = $objTask->checkAddData($taskData);
        if ($checkResult['status'] == Task::STATUS_FAIL){
            return false;
        }
        $ret = $this->getStructure()->addTask($objTask);

        return $ret;
    }

    /**
     * 批量入队列
     *
     * @param $arrTaskList
     * @return mixed
     * @throws \Exception
     */
    public function addTaskList($arrTaskList)
    {
        $arrCheckParam = array('typeName', 'subTypeName', 'params');
        $arrObjTask = array();
        foreach ($arrTaskList as $task){
            foreach ($arrCheckParam as $item){
                if (!in_array($item, array_keys($task))){
                    throw new \Exception("param error : $item");
                }
            }
            $typeName = $task['typeName'] . '_' . $task['subTypeName'];
            $arrTypeConf = $this->arrSubtype[$typeName];
            $isWriteMysqlLog = isset($arrTypeConf['isWriteMysqlLog']) ? $arrTypeConf['isWriteMysqlLog'] : true;
            $taskData = array();
            $taskData['type'] = $task['typeName'];
            $taskData['sub_type'] = $task['subTypeName'];
            $taskData['param_content'] = $task['params'];
            $taskData['is_write_mysql_log'] = $isWriteMysqlLog;
            isset($task['pKey']) && $taskData['pKey'] = $task['pKey'];
            isset($task['p_key']) && $taskData['pKey'] = $task['p_key'];
            isset($task['preExecTime']) && $taskData['preexec_time'] = $task['preExecTime'];
            $objTask = new Task($this);
            $checkResult = $objTask->checkAddData($taskData);
            if ($checkResult['status'] == Task::STATUS_FAIL){
                throw new \Exception("param error ");
            }
            $arrObjTask[] = $objTask;
        }
        $ret = $this->getStructure()->addTaskList($arrObjTask);

        return $ret;
    }

    public function getTasks($type, $preexec_time = null)
    {
        $intNum = $this->arrProcess[$type]['consumeNum'];
        return $this->getStructure()->getTasks($type, $intNum, $preexec_time);
    }

    /**
     * task状态处理
     *
     * @param $result
     * @param $taskInfo
     * @return mixed
     */
    public function updateTask($result, $taskInfo){
        if ($result['status'] == Task::STATUS_RETRY){
            if ($this->arrTypeSubMap[$taskInfo['type']][$taskInfo['sub_type']]['tryNum'] <= $taskInfo['try_num']){
                $result['status'] = Task::STATUS_FAIL;
                $result['mark'] .= ',has no retry num';
            }else{
                $result['status'] = Task::STATUS_NEW;
                $result['ext']['try_num'] = $taskInfo['try_num']+1;
            }
        }
        isset($result['type']) && $result['ext']['type'] = $result['type'];
        isset($result['sub_type']) && $result['ext']['sub_type'] = $result['sub_type'];

        return $this->getStructure()->updateTask($result['status'], $result['mark'], $result['id'], $result['ext']);
    }

    /**
     * 判断任务是否存在，且未消费
     *
     * @param $typeName
     * @param $pKey
     * @return bool
     */
    public function hasTask($typeName, $pKey)
    {
        list($typeName, $subTypeName) = explode('_', $typeName);
        $taskData['type'] = $typeName;
        $taskData['sub_type'] = $subTypeName;
        $taskData['param_content'] = array();
        $taskData['pKey'] = $pKey;
        $objTask = new Task($this);
        $checkResult = $objTask->checkSearchData($taskData);
        if ($checkResult['status'] == Task::STATUS_FAIL){
            return false;
        }
        $ret = $this->getStructure()->hasTask($objTask);

        return $ret;
    }

    /**
     * 释放超时的队列
     * @param $type
     * @return mixed
     */
    public function resetHoldMess($type) {
        $arrNowPid = $this->getConnIdList();

        return $this->getStructure()->resetHoldMess($arrNowPid, $type, $this->arrTypeSubMap);
    }

    /**
     * 平滑退出复位未消费状态
     *
     * @param $type
     * @return bool
     */
    public function quitDeal($type) {
        return $this->getStructure()->quitDeal($type);
    }

    /**
     * 获取消费比率
     * @return mixed
     */
    public function getConsumeRatio(){
        $arrPid = $this->getConnIdList();
        $arrProcess = $this->arrProcess;
        if (isset($arrProcess['queue_timer_process'])){
            unset($arrProcess['queue_timer_process']);
        }
        $arrQueueType = ToolArray::getFieldValue($arrProcess, 'type');
        return $this->getStructure()->getConsumeRatio($arrPid, $arrQueueType);
    }

    /**
     * 写log
     *
     * @param $strLog
     * @param array $arrParams
     */
    public function log($strLog, $arrParams=array()){
        $this->objLog->log($strLog, $arrParams);
    }

    /**
     * 获取master开启的子进程id
     * @param bool $isMaster
     * @return array
     */
    public function getConnIdList($isMaster = false){
        $workerName = $this->scriptName;
        if (function_exists('cli_set_process_title')) {
            $cmd = "ps -ef | grep '$workerName' | grep -v grep | grep -v /bin/sh | awk '{print $3,$8,$2}' ";
        }else{
            $cmd = "ps -ef | grep '$workerName' | grep -v grep | grep -v /bin/sh | awk '{print $3,$9,$2}' ";
        }
        $fp = popen($cmd, 'r');
        if ($isMaster){
            $currentPid=posix_getpid();
        }else{
            $currentPid=posix_getppid();
        }
        $arrPid = array();
        while (!feof($fp) && $fp) {
            $_line = trim(fgets($fp, 1024));
            $arr = explode(" ",$_line);
            /*if(trim($arr[0])==$currentPid && preg_match("/^{$workerName}/",trim($arr[1]))){
                $arrPid[] = $arr[2];strpos(trim($arr[1]),$workerName) !== false
            }*/
            if(trim($arr[0])==$currentPid){
                $arrPid[] = $arr[2];
            }
        }
        fclose($fp);

        return $arrPid;
    }
}
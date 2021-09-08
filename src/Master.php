<?php
/**
 * master process
 * User: javion
 */
namespace Javion\cu_queue;

use Javion\cu_queue\Lib\Dinger;
use Javion\cu_queue\Lib\ToolArray;

class Master{
    //是否停止
    static $stop = 0;

    //队列类
    public $objQueue = null;
    protected $process = [];
    //子进程pid数组
    protected $child   = [];
    //主进程超时时间
    protected $overTime = 0;
    //主进程运行时间
    protected $startTime;
    //子进程超时时间
    protected $childOverTime = 2400;
    //timer进程运行时间
    protected $timeOverTime = 3600;
    //主进程闹钟
    protected $alarm_time = 30;
    //各类型队列堆积计算数据
    protected $hasItem = [];
    //各类型进程数量记录数组
    private $arrCurNum = [];
    //debug
    protected $isDebug = false;
    //log文件
    protected $strLogFile = null;
    //计算后的最大进程数信息数组
    private $arrMaxChildProcess = array();
    //timer进程
    private $timerProcess = null;
    //管道文件
    private $pipePath = '';

    private $maxChildNum = 10;
    private $minChildNum = 1;

    public function __construct(Queue $objQueue)
    {
        if (!function_exists('pcntl_fork')) {
            die("pcntl_fork not existing");
        }
        $this->objQueue  = $objQueue;
        $this->overTime = $objQueue->masterExecTime;
        $this->childOverTime = $objQueue->wokerExecTime;
        $this->startTime = time();
        $this->process = $objQueue->arrProcess;
        //$this->pipePath = sprintf('%s_queue_%s_%s_pipe_file.txt', $this->objQueue->pipePath, posix_getpid(), $this->startTime);
        $this->pipePath = $this->objQueue->pipePath;
        //赋值一个timer进程
        $timerProcess = array(
            'type' => 'queue_timer_process',
            'maxProcessNum' => 1,
            'minProcessNum' => 1,
            'thresholdNum' => 1,
            'typeName' => 'queue_timer_process',
        );
        $this->process['queue_timer_process'] = $timerProcess;
        $this->init();
    }
    //初始化
    private function init(){
        $arrNeedKey = array('type', 'thresholdNum');
        foreach ($this->process as $k => $item) {
            $arrKey = array_keys($item);
            if (count(array_intersect($arrNeedKey, $arrKey)) != 2){
                die("process error");
            }
            if (!in_array('maxProcessNum', $arrKey)) {
                $this->process[$k]['maxProcessNum'] = $this->maxChildNum;
            }
            if (!in_array('minProcessNum', $arrKey)) {
                $this->process[$k]['minProcessNum'] = $this->minChildNum;
            }
            $this->hasItem[$k] = array(
                'type' => $k,
                'ratio' => 1,
                'leftNum' => 0
            );
            $this->arrMaxChildProcess[$k] = array(
                'maxProcessNum' =>  $this->process[$k]['minProcessNum'],
                'ratio' => 0
            );
        }
    }

    /**
     * 设置子进程
     * @param $process
     */
    public function setProcess($process)
    {
        $this->process = $process;
    }

    /**
     * 设置检测时间间隔 单位s
     * @param $time
     */
    public function setAlarmTime($time){
        $this->alarm_time = $time;
    }

    /**
     * fork 子进程
     */
    protected function forkProcess()
    {
        //循环创建每个type 的消费子进程
        $process  = $this->process;
        foreach($process as $key => $item) {
            $num = $item['minProcessNum'];
            if (is_numeric($key)){
                for ($i = 0; $i < $num; $i++){
                    $this->forkWorkerProcess($item['type'], $i+1, $item['typeName']);
                    sleep(2);
                }
            }elseif ($key == 'queue_timer_process'){
                $this->forkTimerProcess();
            }
        }

        return $this;
    }

    /**
     * 创建worker子进程操作
     * @param $type
     * @param $num
     * @param $typeName
     * @return $this
     */
    private function forkWorkerProcess($type, $num, $typeName)
    {
        $pid = pcntl_fork();
        if ($pid == 0) {
            $ppId = posix_getppid();
            $workerName = sprintf('%s_%s_%s_%d', $this->objQueue->scriptName, $this->objQueue->workerName, $typeName, $num);
            $objQueue = serialize($this->objQueue);
            $objQueue = unserialize($objQueue);
            $worker = new Worker($objQueue, $workerName, $type);
            $worker->setMasterStatus(static::$stop);
            $worker->setMasterPid($ppId);
            $worker->run();
            exit(0);
        } else if ($pid > 0) {
            //记录子进程信息
            $childProcess = array(
                'pid' => $pid,
                'type' => $type,
                'type_name' => $typeName,
                'create_time' => time()
            );
            $this->child[$pid] = $childProcess;
            if (!isset($this->arrCurNum[$type])){
                $this->arrCurNum[$type] = 0;
            }
            $this->arrCurNum[$type]++;
        }else{
            $this->objQueue->log("创建进程失败");
        }
        return $this;
    }
    /**
     * 创建timer子进程操作
     */
    private function forkTimerProcess()
    {
        $pid = pcntl_fork();
        if ($pid == 0) {
            $ppId = posix_getppid();
            $timerName = sprintf('%s_%s', $this->objQueue->scriptName, 'timer');
            $timer = new Timer($this->objQueue, $timerName);
            $timer->setMasterStatus(0);
            $timer->setMasterPid($ppId);
            $timer->setPipeFile($this->pipePath);
            $timer->run();
            exit(0);
        } else if ($pid > 0) {
            //记录子进程信息
            $childProcess = array(
                'pid' => $pid,
                'type' => 'queue_timer_process',
                'type_name' => 'queue_timer_process',
                'create_time' => time()
            );
            $this->child[$pid] = $this->timerProcess = $childProcess;
            $this->arrCurNum['queue_timer_process'] = 1;
        }else{
            $this->objQueue->log("创建进程失败");
        }
        return $this;
    }

    /**
     * 等待子进程结束
     */
    protected function waiteProcess()
    {
        while(count($this->child)) {
            foreach($this->child as $pid => $item){
                //检测配置文件是否改动
                if (!(static::$stop) && $this->checkConfigChanged()){
                    $this->objQueue->log("the config file changes");
                    $this->sigQuitHandler(SIGTERM);
                }
                //回收子进程
                $res = pcntl_waitpid($pid, $status,WNOHANG);
                pcntl_signal_dispatch();
                $leftTime = time() - $this->startTime;
                if ( -1 == $res || $res > 0 ) {
                    unset($this->child[$pid]);
                    $this->objQueue->log("pid $pid 退出", array("pid" => $pid, "type" => $item['type_name']));
                    //进程数扣除
                    if (isset($this->arrCurNum[$item['type']]) && $this->arrCurNum[$item['type']] > 0){
                        $this->arrCurNum[$item['type']]--;
                    }
                    if ($item['type'] == 'queue_timer_process'){
                        $this->timerProcess = null;
                    }
                }//判断子进程是否存在且超时，超过时限运行1.5倍时间则强制退出
                elseif (posix_kill($pid, 0) ){
                    $execMaxTime = $this->childOverTime*1.5;
                    $item['type'] == 'queue_timer_process' && $execMaxTime = $this->timeOverTime*1.5;
                    if ((time() - $item['create_time']) > $execMaxTime){
                        exec("kill -9 $pid");
                        $this->objQueue->log("pid $pid 退出, 超时退出", array('pid' => $pid, 'create_time' => $item['create_time'], 'type' => $item['type']));
                    }
                    if (static::$stop == 1){
                        posix_kill($pid,SIGTERM);
                        $this->objQueue->log("master stop, send SIGTERM to {$pid}", array('pid' => $pid, 'create_time' => $item['create_time'], 'type' => $item['type']));
                    }
                }
                /**
                 * 子进程重新拉起条件（至少保证运行时间内存在一个进程）：
                 * 1、主进程未超时
                 * 2、每个type类型子进程数小于当前可拉起的最大值
                 * 3、队列数据不为空 / 最多1个timer进程
                 * 4、$stop 状态值为0
                 */
                $maxProcessNum = $this->arrMaxChildProcess[$item['type']]['maxProcessNum'];
                //$this->objQueue->log("可创建最大数量", array('type' => $item['type'], 'max' => $maxProcessNum, 'cur' => $this->arrCurNum[$item['type']]));
                if (($this->overTime > $leftTime) && !static::$stop && isset($this->arrCurNum[$item['type']]) &&
                    ($this->arrCurNum[$item['type']] == 0 || (isset($this->hasItem[$item['type']]['leftNum']) && $this->hasItem[$item['type']]['leftNum'] && $this->arrCurNum[$item['type']] < $maxProcessNum))){
                    if ($item['type'] == 'queue_timer_process'){
                        $this->forkTimerProcess();
                    }else{
                        $this->forkWorkerProcess($item['type'], $this->arrCurNum[$item['type']] + 1, $item['type_name']);
                    }
                    $this->objQueue->log("创建新进程", array('type' => $item['type_name']));
                }
            }
            usleep(50000);
        }

        return $this;
    }

    /**
     * 获取可拉起的最大进程数
     * @param $type
     * @param int $ratio
     * @return float|int
     */
    protected function getMaxChildProcessNum($type, $ratio = 1){
        $process = $this->process;
        $process = ToolArray::getMapFromList($process, 'type');
        $num = 1;
        if (isset($process[$type]) && isset($this->hasItem[$type]['leftNum']) && $this->hasItem[$type]['leftNum'] != 0){
            $minNum = $process[$type]['minProcessNum'];
            //$curNum = isset($this->arrCurNum[$type]) ? $this->arrCurNum[$type] : $minNum;
            $maxNum = $process[$type]['maxProcessNum'];
            //$ratio = $this->hasItem[$type]['ratio'];
            //$num = floor($curNum * $ratio);
            $num = floor($maxNum * $ratio);
            $num < $minNum && $num = $minNum;
            $num > $process[$type]['maxProcessNum'] && $num = $process[$type]['maxProcessNum'];
            $this->hasItem[$type]['leftNum'] >= $process[$type]['thresholdNum'] && $num = $process[$type]['maxProcessNum'];
        }

        return $num;
    }

    /**
     * 启动
     */
    public function runProcess() {
        if (function_exists('cli_set_process_title')) {
            //检测master是否运行
            $this->checkSelf();
        }
        //注册信号
        $this->installMasterSignal();
        //php5.5版本以上才有这个函数，踩个坑
        if (function_exists('cli_set_process_title')) {
            //设置master名称
            $masterName = sprintf('%s_%s', $this->objQueue->scriptName, $this->objQueue->masterName);
            cli_set_process_title($masterName);
        }
        //run
        $leftTime = time() - $this->startTime;
        while(($this->overTime ==0 || $this->overTime > $leftTime)){
            //是否退出
            if (static::$stop){
                break;
            }
            $this->objQueue->log("新进程processlist");
            $this->forkProcess()->waiteProcess();
            $leftTime = time() - $this->startTime;
            sleep(5);
        }
        //再次检测回收子进程，避免成为僵死进程
        pcntl_waitpid(0, $status,WNOHANG);
    }

    /**
     * 注册master进程信号
     */
    private function installMasterSignal(){
        pcntl_signal(SIGALRM, array($this, 'sigTimeHandler'));
        pcntl_signal(SIGINT, array($this,'sigQuitHandler'));
        pcntl_signal(SIGTERM, array($this,'sigQuitHandler'));
        pcntl_signal(SIGQUIT, array($this,'sigQuitHandler'));
        pcntl_signal(SIGUSR1, array($this, 'sigTimeHandler'));
        //闹钟
        //pcntl_alarm($this->alarm_time);
    }

    /**
     *  队列检测
     */
    public function sigTimeHandler(){
        static $lastNoticTime = 0;
        if (empty($lastNoticTime)){
            $lastNoticTime = time();
        }
        //$this->hasItem = $this->objQueue->getConsumeRatio($this->startTime);
        $content = file_get_contents($this->pipePath);
        $content = unserialize($content);
        if ($content){
            $this->hasItem = $content;
        }else{
            return;
        }
        $this->objQueue->log("get SIGUSR1", array('item' => $this->hasItem));
        $arrPid = $this->objQueue->getConnIdList(true);
        foreach ($this->hasItem as $type => $item){
            //计算最大进程值
            $this->arrMaxChildProcess[$type]['ratio'] = $item['ratio'];
            $this->arrMaxChildProcess[$type]['maxProcessNum'] = $this->getMaxChildProcessNum($type, $item['ratio']);
            //唤醒子进程
            if ($item['leftNum'] <= 0){
                continue;
            }
            foreach ($this->child as $process){
                if ($process['type'] == $type && in_array($process['pid'], $arrPid)){
                    posix_kill($process['pid'], SIGUSR2);
                    $this->objQueue->log("send SIGUSR2 to {$process['pid']}", array('pid' => $process['pid']));
                }
            }
            //超过最大值，判断是否钉钉通知
            if (false && ($item['leftNum'] > $this->process[$type]['thresholdNum']) &&
                (isset($this->process[$type]['isMoreNotice']) && $this->process[$type]['isMoreNotice']) &&
                (time() - $lastNoticTime >= 30*60)) {
                $lastNoticTime = time();
                $this->dingdingNotice($this->process[$type]['typeName'], $item['leftNum'], $this->arrCurNum[$type], $this->arrMaxChildProcess[$type]['maxProcessNum']);
            }
        }
        //pcntl_alarm($this->alarm_time);
    }

    /**
     *  退出信号处理
     *
     * @param $intSign
     */
    private function sigQuitHandler($intSign) {
        //设置停止状态
        $this->objQueue->log('master process accept quiet_exit sig，pid='.posix_getpid());
        //向子进程发送退出信号
        $arrPid = $this->objQueue->getConnIdList(true);
        foreach ($this->child as $arrProcess){
            $intPid=$arrProcess['pid'];
            if (!in_array($intPid, $arrPid)){
                continue;
            }
            posix_kill($intPid,$intSign);
            $this->objQueue->log("send {$intSign} to {$intPid}", array('pid' => $intPid));
        }
        static::$stop = 1;
    }

    /**
     * 检测master是否运行
     */
    private function checkSelf() {
        if (function_exists('cli_set_process_title')) {
            $masterName = sprintf('%s_%s', $this->objQueue->scriptName, $this->objQueue->masterName);
        }else{
            $masterName = $this->objQueue->scriptName;
        }
        $cmd = "ps -ef | grep '$masterName' | grep -v grep | awk '{print $3,$8,$2}' ";
        $fp = popen($cmd, 'r');
        $boolExit = false;
        while (!feof($fp) && $fp) {
            $_line = trim(fgets($fp, 1024));
            if(empty($_line)){
                break;
            }
            $arr = explode(" ",$_line);
            if(trim($arr[1]) == $masterName){
                $boolExit = true;
            }
        }
        fclose($fp);
        if ($boolExit){
            $this->objQueue->log("master process is running", array('master' => $masterName));
            exit(0);
        }
    }

    /**
     * 检测配置是否发送变动
     * @return bool
     */
    private function checkConfigChanged(){
        static $md5 = '';
        $config = $this->objQueue->getConfigPath();
        $content = md5(file_get_contents($config));
        if (empty($md5)){
            $md5 = $content;
        }
        if($md5 != $content){
            $md5 = $content;
            return true;
        }
        return false;
    }

    /**
     * 发钉钉通知
     *
     * @param $strQueueName
     * @param $intAll
     * @param $intCur
     * @param $intMax
     */
    private function dingdingNotice($strQueueName, $intAll, $intCur, $intMax){
        $arrErrorMsg[] = "【队列积压通知】队列类型：{$strQueueName},当前积压数：{$intAll},当前消费进程数：{$intCur},持续拉起最大进程数：{$intMax}\n";
        $strErrorMsg = implode("\n", $arrErrorMsg);
        $objDinger = new Dinger("queue");
        $objDinger->sendMsg($strErrorMsg);
    }
}
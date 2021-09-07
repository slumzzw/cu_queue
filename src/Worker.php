<?php
/**
 * worker process
 * User: javion
 */
namespace Javion\cu_queue;

use Javion\cu_queue\Lib\ToolTime;

class Worker{
    const STATUS_RUN = 1;
    const STATUS_SLEEP = 2;
    //master status
    private $masterStop = 0;
    //master 进程id
    private $masterPid = 0;
    //queue 对象
    private $objQueue = null;
    //worker name
    private $workerName = '';
    //queue type
    private $queueType;
    //worker status
    private $workerStatus;

    public function __construct(Queue $objQueue, $workerName, $queueType)
    {
        $this->objQueue = $objQueue;
        $this->workerName = $workerName;
        $this->queueType = $queueType;
        $this->workerStatus = self::STATUS_RUN;
        //php5.5版本以上才有这个函数，踩个坑
        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($workerName);
        }

    }

    public function setMasterStatus($status){
        $this->masterStop = $status;
    }

    public function setMasterPid($pid) {
        $this->masterPid = $pid;
    }

    public function run(){
        try {
            $this->objQueue->getStructure()->initDeal();
            $this->installWorkerSignal();
            $doTime = time();
            $tryNum = 0;
            $arrQueueList = array();
            //重置
            $this->objQueue->resetHoldMess($this->queueType);
            $preexec_time = null;
            while ((time() - $doTime) < $this->objQueue->wokerExecTime){
                pcntl_signal_dispatch();
                if ($this->checkQuit()) {
                    break;
                }
                //获取队列信息
                (self::STATUS_RUN == $this->workerStatus) && $arrQueueList = $this->objQueue->getTasks($this->queueType, $preexec_time);
                $this->objQueue->log('队列获取结果', array('num'=>count($arrQueueList), 'worker_status'=>$this->workerStatus));
                if (empty($arrQueueList) || self::STATUS_SLEEP == $this->workerStatus){
                    $this->objQueue->log('has no task', array('tryNum'=>$tryNum));
                    $arrQueueList = array();
                    $tryNum++;
                    $preexec_time = null;
                    if ($tryNum > 200){
                        break;
                    }
                    if ($tryNum > 15 && $this->objQueue->getStructure()->getStructureName() == 'mysql'){
                        $this->workerStatus = self::STATUS_SLEEP;
                    }
                    if ($this->objQueue->getStructure()->getStructureName() == 'mysql') {
                        sleep(3);
                        continue;
                    }else{
                        sleep(1);
                        continue;
                    }

                }
                $tryNum = 0;
                $objTask = new Task($this->objQueue);
                foreach($arrQueueList as $arrMess) {
                    $objTimer = new ToolTime(true);
                    $this->objQueue->log('start process mess', array('id'=>$arrMess['id'], 'queue_type' => $this->queueType));
                    $ret = $objTask->run($arrMess);
                    $ret['id'] = $arrMess['id'];
                    $this->objQueue->updateTask($ret, $arrMess);
                    $this->objQueue->log('LOG process one mess finish', array('id'=>$arrMess['id'], 'queue_type' => $this->queueType, 'use_time'=>$objTimer->stop()));
                    if (empty($preexec_time)|| (isset($arrMess['preexec_time']) && $preexec_time < $arrMess['preexec_time'])){
                        isset($arrMess['preexec_time']) && $preexec_time =  isset($arrMess['preexec_time']);
                    }
                    pcntl_signal_dispatch();
                    usleep(50000);
                    if ($this->checkQuit()) {
                        break;
                    }
                }
            }
            $this->objQueue->quitDeal($this->queueType);
        } catch (\Exception $e) {
            $ret['errno'] = $e->getCode();
            echo json_encode($ret['errno']), PHP_EOL;
        }
    }

    /**
     * 检测是否需要退出
     *
     * @return bool
     */
    private function checkQuit(){
        if ($this->masterStop){
            return true;
        }
        //获取当前父进程id
        $ppid = posix_getppid();
        if ($this->masterPid != $ppid){
            return true;
        }

        return false;
    }

    /**
     * 注册worker进程信号
     */
    private function installWorkerSignal(){
        pcntl_signal(SIGINT, array($this,'sigQuitHandler'));
        pcntl_signal(SIGTERM, array($this,'sigQuitHandler'));
        pcntl_signal(SIGQUIT, array($this,'sigQuitHandler'));
        pcntl_signal(SIGUSR2, array($this,'sigUsrHandler'));
    }

    private function sigQuitHandler(){
        $this->masterStop = 1;
        $this->workerStatus = self::STATUS_SLEEP;
    }

    private function sigUsrHandler() {
        if ($this->masterStop){
            $this->workerStatus = self::STATUS_SLEEP;
        }else{
            $this->objQueue->log('get usr2 sig');
            $this->workerStatus = self::STATUS_RUN;
        }
    }
}

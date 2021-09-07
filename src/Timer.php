<?php
/**
 * timer process
 * User: javion
 */
namespace Javion\cu_queue;

class Timer{
    //process time
    private $maxTime = 3600;
    //master status
    private $masterStop = 0;
    //master 进程id
    private $masterPid = 0;
    //queue 对象
    private $objQueue = null;
    //管道文件
    private $pipePath = '';
    //检测时间 秒
    protected $alarm_time = 10;

    public function __construct(Queue $objQueue, $workerName)
    {
        $this->objQueue = $objQueue;
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

    public function setPipeFile($pipePath){
        $this->pipePath = $pipePath;
    }

    public function run(){
        try {
            $this->installWorkerSignal();
            $doTime = time();
            sleep(5);
            //创建管道
            while ((time() - $doTime) < $this->maxTime) {
                pcntl_signal_dispatch();
                if ($this->checkQuit()) {
                    break;
                }
                $hasItem = $this->objQueue->getConsumeRatio();
                file_put_contents($this->pipePath, serialize($hasItem));
                //$this->objQueue->log("队列检测，数据获取");
                sleep(1);
            }
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
        pcntl_signal(SIGALRM, array($this, 'timeHandler'));
        pcntl_signal(SIGUSR2, SIG_IGN);
        //首个时间信号
        pcntl_alarm($this->alarm_time);
    }

    private function sigQuitHandler(){
        $curPid = $this->objQueue->getConnIdList();
        $pid = posix_getpid();
        if (count($curPid) == 1 && $curPid[0] == $pid){
            $this->masterStop = 1;
            $this->objQueue->log("time process need quit", array('pid' => $pid, 'all_p' => json_encode($curPid)));
        }else{
            $this->objQueue->log("wait master", array('pid' => $pid, 'all_p' => json_encode($curPid)));
        }
    }

    //时间信号
    private function timeHandler(){
        posix_kill($this->masterPid, SIGUSR1);
        $this->objQueue->log("send SIGUSR1 to {$this->masterPid}, 队列检测", array('pid' => $this->masterPid));
        //继续发时间信号
        pcntl_alarm($this->alarm_time);
    }
}
<?php
/**
 * log class
 * User: javion
 */
namespace Javion\cu_queue;

class Log{
    private $strLogFile = '';

    public function __construct($strLogFile = '')
    {
        $this->strLogFile = $strLogFile;
    }

    public function setLogFile($strLogFile = ''){
        $this->strLogFile = $strLogFile;
    }

    //写log
    public function log($strLog, $arrParams=array()) {
        foreach($arrParams as $key=>$val) {
            if(!is_scalar($val)) {
                $val = json_encode($val);
            }
            $strLog .= sprintf(' %s[%s]', $key, $val);
        }
        $size = memory_get_usage(true);
        $unit=array('b','kb','mb','gb','tb','pb');
        $strSize = @round($size/pow(1024,($i=floor(log($size,1024)))),2).' '.strtoupper($unit[$i]);
        $strLog = sprintf("%s: %s MEM[%s] PID[%d] ", date('Y-m-d H:i:s'),  $strLog, $strSize, getmypid());
        if($this->strLogFile) {
            file_put_contents($this->strLogFile . '.' . date('Ymd'), $strLog."\n", FILE_APPEND);
        }
        echo $strLog."\n";
    }
}
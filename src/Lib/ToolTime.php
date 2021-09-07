<?php

/**
 * time tool 计时器
 * User: javion
 */
namespace Javion\cu_queue\Lib;
class ToolTime{

    const PRECISION_MS = 1;
    const PRECISION_S = 2;
    const PRECISION_US = 3;

    private $intBegTime = 0;
    private $intTimeUserd = 0;
    private $bolStopped = true;
    private $intPrecision;

    /**
        * @synopsis  __construct 
        *
        * @param $bolStart, 是否立即启动计时
        * @param $intPrecision, 计时单位，默认是ms
        *
        * @returns   
     */
    function __construct($bolStart = false, $intPrecision = self::PRECISION_MS) {
        $this->intPrecision = $intPrecision;

        if($bolStart) {
            $this->start();
        }
    }

    
    function start() {
        if(!$this->bolStopped) {
            return false;
        }

        $this->bolStopped = false;
        $this->intBegTime = self::getTimeStamp(self::PRECISION_US);
        return true;
    }

    function stop() {
        if($this->bolStopped) {
            return false;
        }

        $this->bolStopped = true;
        $thisTime = self::getTimeStamp(self::PRECISION_US) - $this->intBegTime;
        $this->intTimeUserd += $thisTime;

        switch($this->intPrecision) {
        case self::PRECISION_MS:
            return intval($thisTime/1000);

        case self::PRECISION_S:
            return intval($thisTime/1000000);

        default:
            return $thisTime;
        }
    }

    function reset() {
        $this->intBegTime = 0;
        $this->intTimeUserd = 0;
        $this->bolStopped = true;
    }

    function getTotalTime($intPrecision = null) {
        if($intPrecision === null) {
            $intPrecision = $this->intPrecision;
        }

        switch($intPrecision) {
        case self::PRECISION_MS:
            return intval($this->intTimeUserd/1000);

        case self::PRECISION_S:
            return intval($this->intTimeUserd/1000000);

        default:
            return $this->intTimeUserd;
        }
    }

    static function getTimeStamp($intPrecision = self::PRECISION_MS) {
        switch($intPrecision) {
        case self::PRECISION_MS:
            return intval(microtime(true)*1000);

        case self::PRECISION_S:
            return time();

        case self::PRECISION_US:
            return intval(microtime(true)*1000000);

        default:
            return 0;
        }
    }
}

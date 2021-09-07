<?php
/**
 * Dinger 钉钉机器人公用类
 * User: javion
 */
namespace Javion\cu_queue\Lib;
class Dinger{
    private $strWebHook = null;
    protected $bolAtAll = false;
    public function __construct($strDinger){
        $this->_init($strDinger);
    }

    private function _init($strDinger){
        $arrDingConf = Glo_Conf::getConfArray('dingding.ini', $strDinger);//需在conf/dingding.ini文件中有对应配置项
        if ($arrDingConf == false || !isset($arrDingConf['webhook'])){
            return;
        }

        $this->strWebHook = $arrDingConf['webhook'];
    }

    //设置@所有人
    public function setAtAll($bolAtAll){
        if ($bolAtAll === true) {
            $this->bolAtAll = true;
        } else {
            $this->bolAtAll = false;
        }
    }

    /**
        * @synopsis  send 
        *
        * @param $arrParams
        *   array(
        *       'msgtype' => 'text',
        *       'text' => array ('content' => "msg"),
        *       'at' => array(
        *           'atMobiles' => array(18826056043), 
        *           'isAtAll' => false,
        *       ),  
        *   );
        *
        * @returns   
        * 同一个机器人一分钟内最多发送20条消息
     */
    public function sendMsg($strContent, $arrAt = array(), $strType = "text"){
        if (empty($strContent) || !is_array($arrAt) || $strType != "text" || empty($this->strWebHook)) {  
            return false;
        }

        $arrData = array(
            'msgtype' => $strType,
            'text' => array(
                'content' => $strContent,
            ),
            'at' => array(
                'atMobiles' => $arrAt,
                'isAtAll' => $this->bolAtAll,
            ),
        );
        $strData = json_encode($arrData);

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $this->strWebHook);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        $arrOption = array(
            CURLOPT_POSTFIELDS => $strData,
            CURLOPT_HTTPHEADER => array ('Content-Type: application/json;charset=utf-8'), 
        );
        curl_setopt_array($ch, $arrOption);
        curl_setopt($ch, CURLOPT_POST, 1);
        $res = curl_exec($ch);
        curl_close($ch);
        $arrRes = json_decode($res, true);
        if (isset($arrRes['errcode']) && $arrRes['errcode'] == 0) {
            return true;
        }
        return false;
 
    }
}

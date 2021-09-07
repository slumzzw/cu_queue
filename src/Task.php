<?php
/**
 * task
 * User: javion
 */
namespace Javion\cu_queue;

class Task{
    //状态
    const STATUS_NEW = 0; //未消费
    const STATUS_PROCESS = 1; //消费中
    const STATUS_SUCCESS = 2; //消费成功
    const STATUS_FAIL = 3; //消费失败
    const STATUS_RETRY = 4; //待重试消费

    //queue 对象
    private $objQueue = null;

    //属性
    private $type;
    private $subType;
    private $paramContent;
    private $pKey;
    private $level = 0;
    private $preexecTime;
    private $strClass;
    private $strMethod;
    private $typeName;
    private $isWriteMysql = true; //是否写mysql，rabbitmq使用

    //结果
    private $result = array(
        'status' => self::STATUS_SUCCESS,
        'mark' => 'success',
        'ext' => array()
    );

    public function __construct(Queue $objQueue)
    {
        $this->objQueue = $objQueue;
    }

    public function getType(){
        return $this->type;
    }

    public function getSubType() {
        return $this->subType;
    }

    public function getTypeName(){
        return $this->typeName;
    }

    public function getParamContent() {
        return json_encode($this->paramContent);
    }

    public function getPkey(){
        return $this->pKey;
    }

    public function getLevel() {
        return $this->level;
    }

    public function getPreexecTime() {
        return $this->preexecTime ? $this->preexecTime : time();
    }

    public function getIsWriteMysql(){
        return $this->isWriteMysql;
    }

    private function checkData($data){
        if (empty($this->objQueue)){
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'objqueue is null',
                'ext' => array()
            );
            return $this;
        }
        if (!isset($data['type'])) {
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'type is null',
                'ext' => array()
            );
            return $this;
        }
        if (!isset($data['sub_type'])) {
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'sub_type is null',
                'ext' => array()
            );
            return $this;
        }
        if (!isset($data['param_content'])) {
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'param_content is null',
                'ext' => array()
            );
            return $this;
        }

        return $this;
    }

    private function checkParam($arrParam, $arrCheck){
        foreach ($arrCheck as $item){
            if (!isset($arrParam[$item])){
                $this->objQueue->log("param error: {$item} is null");
                $this->result = array(
                    'status' => self::STATUS_FAIL,
                    'mark' => " param error: {$item}is null",
                    'ext' => array()
                );
                return $this;
            }
        }
        return $this;
    }

    /**
     * 检测读取的数据
     *
     * @param $data
     * @return array
     */
    public function checkGetData($data){
        $this->checkData($data);
        if (self::STATUS_FAIL == $this->result['status']){
            return $this->result;
        }
        if (!isset($this->objQueue->arrTypeSubMap[$data['type']][$data['sub_type']])){
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'sub_type config is null',
                'ext' => array()
            );
            return $this->result;
        }
        $this->checkParam(json_decode($data['param_content'], true),
            $this->objQueue->arrTypeSubMap[$data['type']][$data['sub_type']]['checkParams']);
        $this->type = $data['type'];
        $this->subType = $data['sub_type'];
        $this->typeName = $this->objQueue->arrTypeSubMap[$data['type']][$data['sub_type']]['typeName'];
        $this->strClass = $this->objQueue->arrTypeSubMap[$data['type']][$data['sub_type']]['class'];
        $this->strMethod = $this->objQueue->arrTypeSubMap[$data['type']][$data['sub_type']]['method'];
        $this->paramContent = json_decode($data['param_content'], true);
        $this->paramContent['queue_id'] = $data['id'];
        $this->paramContent['p_key'] = $data['p_key'];

        return $this->result;
    }

    /**
     * 检测写入的数据
     * @param $data
     * @return array
     */
    public function checkAddData($data){
        $this->checkData($data);
        if (self::STATUS_FAIL == $this->result['status']){
            return $this->result;
        }
        $section = sprintf('%s_%s', $data['type'], $data['sub_type']);
        if (!isset($this->objQueue->arrSubtype[$section])){
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'sub_type config is null',
                'ext' => array()
            );
            return $this->result;
        }
        $arrTask = $this->objQueue->arrSubtype[$section];
        $this->typeName = $arrTask['typeName'];
        $this->type = $arrTask['type'];
        $this->subType = $arrTask['subType'];
        $this->level = $arrTask['level'];
        $this->strClass = $arrTask['class'];
        $this->strMethod = $arrTask['method'];
        $this->paramContent = $data['param_content'];
        $this->pKey = isset($data['pKey']) ? $data['pKey'] : md5(json_encode($data['param_content']));
        $this->preexecTime = isset($data['preexec_time']) && $data['preexec_time'] ? $data['preexec_time'] : time();
        isset($data['is_write_mysql_log']) && $this->isWriteMysql = $data['is_write_mysql_log'];

        return $this->result;
    }

    /**
     * 检测搜索的数据
     * @param $data
     * @return array
     */
    public function checkSearchData($data){
        $this->checkData($data);
        if (self::STATUS_FAIL == $this->result['status']){
            return $this->result;
        }
        $section = sprintf('%s_%s', $data['type'], $data['sub_type']);
        if (!isset($this->objQueue->arrSubtype[$section])){
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => 'sub_type config is null',
                'ext' => array()
            );
            return $this->result;
        }
        $arrTask = $this->objQueue->arrSubtype[$section];
        $this->checkParam($data['param_content'], $arrTask['checkParams']);
        $this->type = $arrTask['type'];
        $this->typeName = $arrTask['typeName'];
        $this->subType = $arrTask['subType'];
        $this->level = $arrTask['level'];
        $this->strClass = $arrTask['class'];
        $this->strMethod = $arrTask['method'];
        $this->paramContent = $data['param_content'];
        $this->pKey = isset($data['pKey']) ? $data['pKey'] : md5(json_encode($data['param_content']));
        $this->preexecTime = isset($data['preexec_time']) && $data['preexec_time'] ? $data['preexec_time'] : time();

        return $this->result;
    }

    /**
     * task 执行
     *
     * @param $data
     * @return array
     */
    public function run($data){
        $this->result = array(
            'status' => self::STATUS_SUCCESS,
            'mark' => 'success',
            'ext' => array()
        );
        $this->checkGetData($data);
        if (self::STATUS_FAIL == $this->result['status']){
            return $this->result;
        }
        if (empty($this->strClass) || empty($this->strMethod)) {
            $this->objQueue->log('one mess callback error', array('id'=>$data['id']));
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => " callback error",
                'ext' => array()
            );
            return $this->result;
        }
        if (!class_exists($this->strClass)){
            $this->objQueue->log("class no exist class[{$this->strClass}]", array('id'=>$data['id']));
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => "class no exist class[{$this->strClass}]",
                'ext' => array()
            );
            return $this->result;
        }
        $objClass = new $this->strClass();
        if (!method_exists($objClass, $this->strMethod)){
            $this->objQueue->log("method no exist method[{$this->strMethod}]", array('id'=>$data['id']));
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => "method no exist method[{$this->strMethod}]",
                'ext' => array()
            );
            return $this->result;
        }
        try {
            $ret = call_user_func_array(array($objClass, $this->strMethod), array($this->paramContent));
            if (isset($ret['status']) && is_numeric($ret['status'])){
                $this->objQueue->log('LOG process one mess success', array('id'=>$data['id']));
                $mark = 'call back';
                isset($ret['mark']) && $mark = $ret['mark'];
                $this->result = array(
                    'status' => $ret['status'],
                    'mark' => $mark,
                    'ext' => $ret
                );
                return $this->result;
            }else{
                $this->objQueue->log('LOG process one mess false', array('id'=>$data['id']));
                $this->result = array(
                    'status' => self::STATUS_FAIL,
                    'mark' => "call error,return false",
                    'ext' => array()
                );
                return $this->result;
            }
        } catch (\Exception $e) {
            $ret['errno'] = $e->getMessage();
            $this->result = array(
                'status' => self::STATUS_FAIL,
                'mark' => "call error, {$ret['errno']}",
                'ext' => array()
            );
            $this->objQueue->log('LOG process one mess false', array('id'=>$data['id']));
        }

        return $this->result;
    }

}
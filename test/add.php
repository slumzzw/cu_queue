<?php
/**
 * 入队列
 * User: javion
 */
require_once dirname(__DIR__) . '/vendor/autoload.php';
use Javion\cu_test\Queue;
$objQueue = new Queue();
//单个入队列
$arrParam = array('data' => 'test');
$strKey = '这是一个任务key，表示当前这个任务'; //不传会默认md5生成一个
$intPreExecTime = time(); //设置执行时间，不传默认为立即消费
$objQueue->addTask('test_run', $arrParam, $strKey, $intPreExecTime);

//批量入队列 ypeName和subTypeName拼起来就是配置文件中任务的标识名称 test_run
$arr = array(
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test'), 'pKey' => 'test_4', 'preExecTime' => 1631077834),
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test'), 'pKey' => 'test_5'),
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test')),
);
$objQueue->addTaskList($arr);
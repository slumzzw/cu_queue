<?php
/**
 * 入队列
 * User: javion
 */
require_once dirname(__DIR__) . '/vendor/autoload.php';
use Javion\cu_test\Queue;
$objQueue = new Queue();
//单个入队列
$objQueue->addTask('test_run', array('data' => 'test'), 'test_3', 1631076034);
//批量入队列
$arr = array(
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test'), 'pKey' => 'test_4', 'preExecTime' => 1631077834),
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test'), 'pKey' => 'test_5'),
    array('typeName' => 'test', 'subTypeName' => 'run', 'params' => array('data' => 'test')),
);
$objQueue->addTaskList($arr);
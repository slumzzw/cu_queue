<?php
/**
 * 队列处理类
 * User: javion
 */

namespace Javion\cu_test;


use Javion\cu_queue\Task;

class Deal
{
    public function testQueue($arrParams){
        var_dump($arrParams['data']);
        return array(
            'status' => Task::STATUS_SUCCESS,
            'mark' => 'success'
        );
    }

}
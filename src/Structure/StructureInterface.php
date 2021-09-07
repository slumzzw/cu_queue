<?php
/**
 * structure interface
 * User: javion
 */
namespace Javion\cu_queue\Structure;

use Javion\cu_queue\Log;
use Javion\cu_queue\Task;

interface StructureInterface{
    /**
     * @param array $options
     * @return mixed
     */
    public function setOptions(array $options);

    /**
     * @return mixed
     */
    public function getOptions();

    public function getStructureName();

    public function addTask(Task $task);

    public function addTaskList($arrTaskList);

    public function getTasks($type, $intNum, $preexec_time = null);

    public function updateTask($status, $mark, $id, $arrExt = array());

    public function getConsumeRatio($arrPid, $arrQueueType);

    public function setLogObj(Log $log);

    public function resetHoldMess($arrPid, $type, $arrTypeSubMap);

    public function initDeal();

    public function quitDeal($type);

    public function hasTask(Task $task);
}
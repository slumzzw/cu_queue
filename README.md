# cu-queue
a common used queue tool(一个通用队列工具)

##工具作者及维护者
javion

##工具依赖
- 支持php5.6及以上
- 依赖 catfan/medoo 数据库操作类，版本 v1.7.10
- 依赖 pcntl、posix扩展

##主要功能简述
 1. 可配置化，支持mysql队列，可拓展其他类型mq，如rabbitmq、redis等。（当前只支持了mysql）
 2. 支持进程数据最大最小值配置，根据队列堆积数动态变化进程。
 3. 支持平滑退出和重启
 4. 支持子进程超时强退
 5. 支持延时队列
 6. 消费失败重试，默认3次
 6. 队列堆积数监控/钉钉预警（如有开启，达到阈值半小时通知一次）

##配置文件
队列配置文件见 'src/Config/config.ini', 具体说明如下
```
;基础配置
[base]
;队列类型，当前支持的值有：mysql
structure = mysql
;-----------------------------------分隔线 mysql队列参数----------------------------------
;队列数据库名
db_connect_name = queue
;队列表名
db_table_name = common_queue
;延时队列表名
db_delay_table_name = common_delay_queue
;mysql数据库连接host
db_host = 172.16.1.1
;mysql数据库连接端口
db_port = 3306
;mysql数据库连接用户名
db_user = queue
;mysql数据库连接密码
db_pwd = 123456
;-----------------------------------分隔线 通用配置参数-----------------------------------
;日志文件
log_file = common_queue.log
;管道文件，需可写权限
pipe_path = /home/www/common_queue.pipe.txt
;master主进程运行时间 秒
master_exec_time = 43200
;worker子进程运行时间 秒
woker_exec_time = 2400
;进程前缀名
script_name = common_used_queue
;master进程名字
master_name = master
;woker进程名字
worker_name = worker
;队列类型 对应下面队列配置，一个队列代表一个topic
queue_type[] = test
;钉钉机器人通知
ding_hook = 'https://oapi.dingtalk.com/robot/send?access_token=修改为自己机器人的token'
;version 版本，如果需要平滑退出，修改版本号即可
version = 1.8

;具体队列配置
;exam对应上面的queue_type 队列类型type=1,对应mysql表的type值 1
[test]
type = 1
;最大进程数
max_process_num = 2
;启动时拉起的最小进程数
min_process_num = 1
;队列堆积数阈值 超过这个值，会直接拉起进程数量，否则动态变化进程数 (附上注释：max是最高值限定)
threshold_num = 2000
;进程一次性hold住队列item数，mysql类型有用
consume_num = 20
;整个topic失败重试次数,可不设置，默认3次
try_num = 5
;该topic 队列消息积压时，是否钉钉通知，默认不通知，设置为1则通知，需参数ding_hook有设置
isMoreNotice = 1

;topic每个小类型配置
;test_run 子类型标识名称，入队列需用到
[test_run:test]
;子类型
sub_type = 1
;消费时调用的具体类及方法
class = \Javion\cu_test\Deal
method = testQueue
;单类型重试次数
try_num = 5

[test_rue:test]
sub_type = 2
class = \Javion\cu_test\Deal
method = testQueue
level = 1
```
##工具主要api
```
	/**
     * 单个入队列
     *
     * @param $typeName /队列类型标识名
     * @param $params   /消费参数
     * @param string $pKey /任务标识
     * @param int $preExecTime /消费时间戳，0表示当前时间
     * @return bool true/false
     */
    public function addTask($typeName, $params, $pKey = '', $preExecTime = 0)；
	
	/**
     * 批量入队列
     *
     * @param $arrTaskList
     * @return mixed
     * @throws Glo_TipError
     */
    public function addTaskList($arrTaskList);
	
	/**
     * 判断是否存在相同的未消费任务
     *
     * @param $typeName /队列类型标识名
     * @param $pKey /任务标识
     * @return bool true存在 false不存在
     */
    public function hasTask($typeName, $pKey);
	
	任务消费方法，返回的格式如下：
	 return array(
       'status' => Task::STATUS_SUCCESS, //消费状态，必须返回
       'mark' => 'success' //消费结果说明，必须返回
	   'preexec_time' => time(), //再次消费时间，重试状态下可以返回，默认当前时间
    );
	
	消费状态枚举值，Task类中：
    const STATUS_NEW = 0; //未消费
    const STATUS_PROCESS = 1; //消费中，队列消费中使用，任务方法不要返回这个状态
    const STATUS_SUCCESS = 2; //消费成功
    const STATUS_FAIL = 3; //消费失败
    const STATUS_RETRY = 4; //待重试消费
```

##使用示例
项目队列类 见 'test/Queue.php'
```
namespace Javion\cu_test;
class Queue extends \Javion\cu_queue\Queue
{
    public function __construct()
    {
        $configPath = dirname(__DIR__) . '/src/Config/config.ini';
        parent::__construct($configPath);
    }
}

```

队列启动, 见 'test/run.php'：
```
<?php
namespace Javion\cu_test;
class Queue extends \Javion\cu_queue\Queue
{
    public function __construct()
    {
        $configPath = dirname(__DIR__) . '/src/Config/config.ini';
        parent::__construct($configPath);
    }
}

```

入队列， 见 'test/add.php'：
```
<?php
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

```

队列平滑重启

- 修改配置即可触动重启（如修改版本version的值）

队列退出

- 关掉crontab定时脚本，然后修改配置文件版本号即可

##注意事项
1. 队列工具会默认拉起一个timer进程，timer进程用于监控队列任务堆积数
2. mysql类型队列有一定的局限性：（1）每天产生的队列任务多时，表数据会快速增长，会影响队列消费效率 （2）同一队列topic多个进程消费时，多个进程hold住任务有一个抢占过程，由于一个任务只会被一个进程获取，抢占行为一定程度上会影响消费效率（特别是任务消费耗时低的情况下）

##sql附录
queue表
```
  CREATE TABLE `common_queue` (
	`id` BIGINT ( 11 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`sub_type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`conn_id` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '消费者标识',
	`param_content` text COMMENT '队列入参',
	`status` TINYINT ( 2 ) NOT NULL DEFAULT '0' COMMENT '0新建 1消费中 2成功 3失败 4需重试',
	`create_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '创建时间',
	`update_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '状态变更时间',
	`preexec_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费时间',
	`p_key` VARCHAR ( 100 ) NOT NULL DEFAULT '' COMMENT '业务唯一标识key，查询用',
	`mark` VARCHAR ( 255 ) NOT NULL DEFAULT '' COMMENT '备注',
	`level` INT ( 11 ) NOT NULL DEFAULT '0',
	`try_num` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '重试次数',
	PRIMARY KEY ( `id` ),
	KEY `indx_s` ( `p_key`, `type` ) USING BTREE,
	KEY `indx_exec` ( `conn_id`, `status` ) USING BTREE,
	KEY `indx_level` ( `level` ) USING BTREE,
	KEY `indx_type_status` ( `type`, `status` ),
	KEY `indx_status` ( `status` ),
    KEY `indx_get` ( `conn_id`, `type`, `status`, `preexec_time` ) USING BTREE
    ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4;
```

延时队列表
```
CREATE TABLE `common_delay_queue` (
	`id` BIGINT ( 20 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`sub_type` TINYINT ( 4 ) NOT NULL DEFAULT '0' COMMENT '队列类型，代码业务备注',
	`param_content` text COMMENT '队列入参',
	`create_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '创建时间',
	`update_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '状态变更时间',
	`preexec_time` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费时间',
	`p_key` VARCHAR ( 100 ) NOT NULL DEFAULT '' COMMENT '业务唯一标识key，查询用',
	`level` INT ( 11 ) NOT NULL DEFAULT '0',
	`try_num` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '重试次数',
	`relate_old_id` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '关联的旧id',
	`preexec_day` INT ( 11 ) NOT NULL DEFAULT '0' COMMENT '预消费日期',
	`is_exec` TINYINT ( 1 ) NOT NULL DEFAULT '0' COMMENT '是否执行 0 否 1执行',
	PRIMARY KEY ( `id` ),
	KEY `indx_day` ( `preexec_time` ) USING BTREE,
	KEY `indx_day_time` ( `preexec_day`, `preexec_time` ) USING BTREE,
	KEY `indx_key` ( `p_key` ) USING BTREE,
	KEY `indx_type` ( `type`, `sub_type` ) USING BTREE
    ) ENGINE = INNODB DEFAULT CHARSET = utf8mb4
```
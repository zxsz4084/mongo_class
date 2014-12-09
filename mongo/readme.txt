ci框架下应用

3个文件 可放于 library/mongo/ 下


配置文件 mongo.php 可放于 config/

<?php
$config['mongo'] = array(
        MESSAGE_MONGO    => array(
                'replicaSet' => 'repl',
                'hosts' =>      array(
                                '192.168.1.1:27017',
                                '192.168.1.2:27017',
                                '192.168.1.3:27017',
                                '192.168.1.4:27017'
                ),
                'db' => 'test'
        ),
        
?>
        
       
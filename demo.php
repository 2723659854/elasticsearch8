<?php

require_once __DIR__.'/Client.php';

$client = new Client(['127.0.0.1:9201'],'elastic', '123456');
//$result = $client->deleteIndex('my_index');
/** 创建表 */
$result = $client->createTable('my_index',  [
    'id' => ['type' => 'long',],
    'title' => ['type' => 'text', "fielddata" => true,],
    'content' => ['type' => 'text', 'fielddata' => true],
    'create_time' => ['type' => 'text'],
    'test_a' => ["type" => "integer"],
    'test_b' => ["type" => "rank_feature", "positive_score_impact" => false],
    'test_c' => ["type" => "rank_feature"],
    'name' => ['type' => 'text', "fielddata" => true,],
    'age' => ['type' => 'integer'],
    'sex' => ['type' => 'integer'],
]);

var_dump($result);exit;


//$all = $client->table('my_index')->getAll();
//var_dump($all);

/** 获取表结构 */
//$result = $client->getMap(['my_index']);



$result = $client->IndexExists('my_index');

/** 批量插入数据链式操作 */
//$result = $client->table('my_index', '_doc')->insertAll([
//    [
//        'id' => rand(1, 99999),
//        'title' => '天有不测风云',
//        'content' => '月有阴晴圆缺',
//        'create_time' => date('Y-m-d H:i:s'),
//        'test_a' => rand(1, 10),
//        'test_b' => rand(1, 10),
//        'test_c' => rand(1, 10),
//        'name' => '张三',
//        'age' => 26,
//        'sex' => 1
//    ]
//]);


///** 查询数据链式操作 */
//$result = $client
//    /** 设置表名 */
//    ->table('my_index','_doc')
//    /** must限制条件 */
//    ->where(['title','=','风云'])
//    /** whereIn查询 */
//    //->whereIn('age',[27])
//    /** whereNotIn查询 */
//    ->whereNotIn('age',[27,29])
//    /** should限制条件 当must和should冲突的时候，must条件优先 */
//    ->orWhere(['test_a','>',1])
//    /** 排序 */
//    ->orderBy('test_a','asc')
//    /** 分页 */
//    ->limit(0,10)
//    /** 筛选查询字段 */
//    ->select(['name','age'])
//    /** 按字段分组 */
//    ->groupBy(['age','sex'])
//    /** 聚合查询 */
//    ->sum(['age'])
//    /** 执行查询操作 */
//    ->getAll();

/** 聚合查询链式操作 */
//$result = $client->table('my_index','_doc')->max(['age'])->getAll();

/** 根据条件更新所有数据链式操作 */
//$result = $client->table('my_index','_doc')->where(['test_a','>',2])->updateAll(['name'=>'陈圆圆']);

/** 获取所有数据 */
//$result = $client->table('my_index','_doc')->getAll();
//var_dump($result);

/** 根据条件删除数据链式操作 */
//$result = $client->table('index','_doc')->where(['test_a','>',2])->deleteAll();
/** 获取所有的index索引 */
//$result = $client->getIndex(['my_index']);
///** 使用id更新数据 */
//$result = $client->table('my_index','_doc')->updateById('ZKVxqZMBa1gEEt8-8pNS',['content'=>'今天你测试了吗']);
///** 使用id 删除数据 */
//$result = $client->table('my_index','_doc')->deleteByIds(['ZKVxqZMBa1gEEt8-8pNS']);
///** 使用id查询数据 */
//$result = $client->table('my_index','_doc')->findById('YqVxqZMBa1gEEt8-Z5NA');
///** 使用id批量查询 */
//$result = $client->table('my_index','_doc')->getByIds(['YqVxqZMBa1gEEt8-Z5NA']);



/** 调用elasticsearch原生方法 */
$params = [
    'index' => 'my_index',
    //'type' =>"_doc",
    //'id' => "YqVxqZMBa1gEEt8-Z5NA",
];
/** 使用原生方法统计满足某条件的数据 */
//$result = $client->count($params);
/** 使用原生方法判断是否存在某一条数据 */
//$result = $client->exists($params);
/** 根据条件删除数据链式操作 */
//$result = $client->table('my_index','_doc')->where(['my_field','=','my_value'])->deleteAll();
/** 添加脚本 这个用不了，不知道咋回事 */
$script = <<<eof
if (doc.containsKey('content') && doc['content'].size() != 0) {
        return doc['content'].value + '_' + '谁不说按家乡好';
      } else {
        return '字段缺失'; // 或者返回其他适当的默认值
      }
eof;
//$result = $client->addScript('update_content',$script);
/** 添加脚本 */
//$result = $client->addScript('update_content2',"(doc['content'].value)+'_'+'abcdefg'");
///** 获取脚本内容 */
//$result = $client->getScript('update_content');
///** 使用脚本查询 */
//$result = $client->table('my_index','_doc')->withScript('update_content')->getAll();
///** 删除脚本*/
//$result = $client->deleteScript('update_content');
/** 原生查询 */
$result = $client->query([
    'index'=>'my_index',
    'body'=>[
        'query'=>[
            'bool'=>[
                'must'=>[
                    [
                        'match_phrase'=>[
                            'title'=>'风云'
                        ],
                    ],
                    [
                        'script'=>[
                            'script'=>"doc['content'].size() != 0"
                        ]
                    ]
                ]
            ]
        ]
    ]
]);
/** 打印处理 结果*/
print_r($result);





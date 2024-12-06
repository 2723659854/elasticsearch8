<?php
require_once __DIR__ . '/vendor/autoload.php';

use Elasticsearch\ClientBuilder;

class Client
{

    /** 客户端 */
    protected $client;

    /** 节点列表 */
    protected $hosts = [];
    /** 登录账户 */
    protected $username;
    /** 登录密码 */
    protected $password;

    /**
     * 实例化客户端
     * @param array $hosts ['127.0.0.1:9201'] 节点列表
     * @param string $username 'elastic' 登录账户
     * @param string $password '123456' 登录密码
     */
    public function __construct(array $hosts, string $username = '', string $password = '')
    {
        /** 节点是必填的 */
        if (empty($hosts)) {
            throw new \InvalidArgumentException('Hosts array cannot be empty');
        }
        /** 保存节点配置 */
        $this->hosts = $hosts;
        $this->username = $username;
        $this->password = $password;
        /** 链接客户端 */
        if (empty($this->client)) {
            $client = ClientBuilder::create()
                ->setHosts($this->hosts);
            if (!empty($this->username) && !empty($this->password)) {
                $client->setBasicAuthentication($this->username, $this->password);
            }
            $this->client = $client->build();
        }
    }

    /** 表名称 */
    protected $table;

    /**
     * 制定表名
     * @param string $table 表名称
     * @return $this
     */
    public function table(string $table)
    {
        $this->table = $table;
        return $this;
    }


    /**
     * 创建表和结构
     * @param string $table = "my_index"
     * @param array $columns = []
     *  <code>
     *   $columns = [
     *      'first_name' => [
     *          'type' => 'text',
     *          'analyzer' => 'standard'
     *          ],
     *      'age' => [
     *          'type' => 'integer'
     *          ]
     *  ]
     *  </code>
     * @return array
     */
    public function createTable(string $table, array $columns)
    {
        if (empty($table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($columns)) {
            throw new \InvalidArgumentException('Columns array cannot be empty');
        }
        $params = [
            'index' => $table,
            'body' => [
                'settings' => [
                    /** 主分片 */
                    'number_of_shards' => 3,
                    /** 负分片 */
                    'number_of_replicas' => 2
                ],
                'mappings' => [
                    '_source' => [
                        /** 保存原始文本 */
                        'enabled' => true
                    ],
                    /** 属性 */
                    'properties' => $columns
                ]
            ]
        ];
        return $this->client->indices()->create($params);
    }

    /**
     * 更新表结构
     * @param string $table = "my_index"
     * @param array $columns = []
     *  <code>
     *   $columns = [
     *      'first_name' => [
     *          'type' => 'text',
     *          'analyzer' => 'standard'
     *          ],
     *      'age' => [
     *          'type' => 'integer'
     *          ]
     *  ]
     *  </code>
     * @return array
     */
    public function updateTable(string $table, array $columns)
    {
        if (empty($table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($columns)) {
            throw new \InvalidArgumentException('Columns array cannot be empty');
        }
        $params = [
            'index' => $table,
            'body' => [
                'settings' => [
                    /** 主分片 */
                    'number_of_shards' => 3,
                    /** 负分片 */
                    'number_of_replicas' => 2
                ],
                'mappings' => [
                    '_source' => [
                        /** 保存原始文本 */
                        'enabled' => true
                    ],
                    /** 属性 */
                    'properties' => $columns
                ]
            ]
        ];
        return $this->client->indices()->putMapping($params);
    }

    /**
     * 获取表结构信息
     * @param array $tables
     * <code>
     *     $tables = ['my_index','my_index2']
     * </code>
     * @return array
     */
    public function getTableInfo(array $tables = [])
    {
        $params = [];
        if (!empty($tables)) {
            $params['index'] = $tables;
        }
        return $this->client->indices()->getMapping($params);
    }

    /**
     * 清理上一次查询的条件，防止污染下一次查询
     * @return void
     */
    protected function clearLastQueryCondition()
    {
        $this->table = null;
    }

    /**
     * 插入数据
     * @param array $data
     * <code>
     *     $data = ['username'=>"张三",'age'=>15]
     * </code>
     * @return array|callable
     */
    public function insert(array $data)
    {
        if (empty($data)) {
            throw new \InvalidArgumentException('Data cannot be empty');
        }
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        $params = [
            'index' => $this->table,
            'body' => []
        ];
        if (isset($data['id'])) {
            $params['id'] = $data['id'];
            unset($data['id']);
        }
        if (empty($data)) {
            throw new \InvalidArgumentException('Data cannot be empty');
        }
        $params['body'] = $data;

        $this->clearLastQueryCondition();
        return $this->client->index($params);
    }

    /**
     * 批量写入数据
     * @param array $data
     * <code>
     *     $data = [
     *      ['username'=>'张三','age'=>12],
     *      ['username'=>'李四','age'=>19],
     *  ]
     * </code>
     * @return array|callable
     */
    public function insertAll(array $data)
    {
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($data)) {
            throw new \InvalidArgumentException('Data cannot be empty');
        }
        $params = [];
        foreach ($data as $row) {
            $params['body'][] = [
                'index' => [
                    '_index' => $this->table,
                ]
            ];
            $params['body'][] = $row;
        }
        $this->clearLastQueryCondition();
        return $this->client->bulk($params);
    }

    /**
     * 通过id查询数据
     * @param string $id
     * @return array|callable
     */
    public function findById(string $id)
    {
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($id)) {
            throw new \InvalidArgumentException('Id cannot be empty');
        }
        $params = [
            'index' => $this->table,
            'id'    => $id
        ];
        $this->clearLastQueryCondition();
        return $this->client->get($params);
    }

    /**
     * 通过id更新数据
     * @param string $id 索引id
     * @param array $data 需要更新的数据
     * <code>
     *     $data = ['username'=>'张三','age'=>12]
     * </code>
     * @return array|callable
     */
    public function updateById(string $id, array $data)
    {
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($id)) {
            throw new \InvalidArgumentException('Id cannot be empty');
        }
        if (empty($data)) {
            throw new \InvalidArgumentException('Data cannot be empty');
        }
        $params = [
            'index' => $this->table,
            'id'    => $id,
            'body'  => [
                'doc' => $data
            ]
        ];
        $this->clearLastQueryCondition();
        return $this->client->update($params);
    }

    /**
     * 通过id删除数据
     * @param string $id 索引id
     * @return array|callable
     */
    public function deleteById(string $id){
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }
        if (empty($id)) {
            throw new \InvalidArgumentException('Id cannot be empty');
        }
        $params = [
            'index' => $this->table,
            'id'    => $id
        ];
        $this->clearLastQueryCondition();
        return $this->client->delete($params);
    }
    /** 搜索条件 */
    protected $where = [];

    /** must查询条件 */
    protected $mustWhere = [];

    /**
     * 构建where条件
     * @param string $key = 'testField'
     * @param mixed $value = 'abc'
     * @return $this
     * @note 放入到must查询中
     */
    public function where(string $key,$value)
    {
        $this->where[$key] = $value;

        return $this;
    }

    public function get()
    {
        if (empty($this->table)) {
            throw new \InvalidArgumentException('Table name cannot be empty');
        }

        if (empty($this->where)) {
            $params = [
                'index' => $this->table,
                'body'  => [
                    'query' => [
                        'matchAll' => new stdClass(),
                    ]
                ]
            ];
        }else{
            $params = [
                'index' => $this->table,
                'body'  => [
                    'query' => [
                        'match' => $this->where,
                    ]
                ]
            ];
        }

        $params = [
            'index' => 'my_index',
            'body'  => [
                'query' => [
                    'bool' => [
                        /** 精确筛选，就是必须满足当前条件 */
                        'filter' => [
                            /** term 精确查询，必须等于abc才算满足 */
                            'term' => [ 'testField' => 'abc' ]
                        ],
                        /** 或者筛选 or 查询  */
                        'should' => [
                            /** match 模糊匹配，只要age包含52就算满足 */
                            'match' => [ 'age' => 52 ]
                        ]
                    ]
                ]
            ]
        ];

        $params = [
            'index' =>'my_index',
            'body'  => [
                'query' => [
                    'bool' => [
                        // 必须满足的条件（相当于AND逻辑），这里尝试精确匹配username为张三
                        'must' => [
                            'term' => [
                                'username' => '张三'
                            ]
                        ],
                        // 可选满足的条件（相当于OR逻辑），这里查询age大于15的数据
                        'should' => [
                            'range' => [
                                'age' => [
                                    'gt' => 15
                                ]
                            ]
                        ],
                        // 表示must中的条件如果不满足，should中的条件也生效（实现了题目要求的逻辑）
                        "minimum_should_match" => 1
                    ]
                ]
            ]
        ];


        /** 构建where条件 */
        $condition = [];


        return $this->client->search($params);
    }



    /** 或者查询 */
    protected $orWhere = [];

    /**
     * 或者查询
     * @param string $key
     * @param mixed $value
     * @return $this
     */
    public function whereOr(string $key,$value){
        $this->orWhere[$key] = $value;
        return $this;
    }

    /** 精确匹配 */
    protected $filterWhere = [];

    /**
     * 精确匹配
     * @param string $key
     * @param mixed $value
     * @return $this
     */
    public function filter(string $key,$value)
    {
        $this->filterWhere[$key] = $value;
        return $this;
    }

    /** 当前页数 */
    protected $page = 1;

    /** 当前 */
    protected $pageSize = 50;

    /**
     * 分页
     * @param int $page 当前页
     * @param int $pageSize 每页条数
     * @return $this
     */
    public function page(int $page= 1, int $pageSize = 50)
    {
        $this->page = $page;
        $this->pageSize = $pageSize;
        return $this;
    }

}
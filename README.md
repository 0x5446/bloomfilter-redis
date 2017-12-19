# 利用Redis实现Bloom-Filter

[TOC]

## 背景

最近有一个项目是点击日志（10亿/天）实时计算，架构上简单来说就是利用flunted去从前端机收集原始日志，然后发给Kafka，Spark消费日志并计算保存结果到Redis。

Kafka的Producer和Consumer端的配置是异步且保证不丢消息，因此当超时发生时，就可能会导致消息的重发或者重复消费，需要在消费环节保证幂等。Spark消费逻辑主要是根据多个维度进行计数计算，因此，我们需要在计算之前去重来保证不重复计数。

考虑到去重数据规模很大，为10亿量级，且我们的业务场景允许FP（False-Positive，假阳性，即实际为非重复数据，被误判为重复数据），因此自然而然考虑到用Bloom-Filter（*布隆过滤器*）这个极其节约空间，且时间复杂度也极低的，存在一定的误判（可控）的算法。

## Bloom-Filter

### 介绍

布隆过滤器（Bloom filter）是由巴顿.布隆于1970年提出的。它实际上是一个很长的二进制向量和一系列随机映射函数。

Bloom filter的思想很简单优雅。我们假设有k个hash function和m位bit的向量filter：
处理输入的过程如下：
1. 使用k个hash函数计算hash值；
2. 将每个hash值对m取余，得到k个在filter中的位置；
3. 将这k个位置的bit置为1

判定一个输入是否在filter中的操作如下：
1. 使用k个hash函数计算hash值；
2. 将每个hash值对m取余，得到k个在filter中的位置；
3. 看所有的位置是不是都是1，如果是返回true，否则返回false

如下图示意：
![image](http://wx4.sinaimg.cn/large/646b377bgy1fmkxnbgtppj20i106hwet.jpg)

### 误判率计算

这里不详细展开False positive的数学分析，只给出结论：
$$P \approx ( 1 - e ^ {- \frac {mk}n}) ^ k$$
当m/n固定时，选择
$$k = \frac nm ln2$$
附近的一个整数，将使P（False positive possibility）最小。[^bfmath]

### 应用场景

给定一个集合S（注意，这里的集合是传统意义上的集合：元素彼此不同。本文不考虑multiset），给定一个元素e，需要判断e∈Se∈S 是否成立。（学术界一般称为membership问题）

- 爬虫：URL是否被爬过（海量url，允许False Positive —— 少一次抓取又何妨）
- 垃圾邮件：全世界至少有几十亿个垃圾邮件地址，大家也都有过误判为垃圾邮件的经历[^sxzm]

实际应用中我们需要针对业务的数据量级和对误判量的要求来选取参数m和k。

下面，我们来看一下不同的m/n,k的条件下的误判率表现。

### False-Positive-Ratio表（含内存空间占用）
设n为**10亿**，设m分别为30、50，k分别为8、16，结果下表：
m | m/n | k | FPR | FPN | Mem
---|---|---|---|---|---
300亿 | 30 | 8 | 9.01e-6 | 9011 | 3.49GB
300亿 | 30 | 16 | 7.26e-7 | 726 | 3.49GB
500亿 | 50 | 8 | 2.28e-7 | 228 | 5.82GB
500亿 | 50 | 16 | 1e-9 | 1 | 5.82GB

10亿为1天的数据量，假设数据24小时均匀分布，那10分钟的数据约为**700万**，设m分别为30、50，k分别为8、16，结果下表：
m | m/n | k | FPR | FPN | Mem
---|---|---|---|---|---
2100万 | 30 | 8 | 9.01e-6 | 63 | 25.03MB
2100万 | 30 | 16 | 7.26e-7 | 5 | 25.03MB
3500万 | 50 | 8 | 2.28e-7 | 1.6 | 41.72MB
3500万 | 50 | 16 | 1e-9 | 0 | 41.72MB

由上述表格，可取m/n为50， k为16，能满足业务要求（误判率：1e-9）。

以上，理论上的准备已经足够充分，后面讲一种基于Redis的通用实现方案。首先我们需要先了解一下Redis的SETBIT方法。

## Redis数据结构String的SETBIT方法

> **SETBIT key offset value**
>
> 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
> 
> 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。
> 
> 当 key 不存在时，自动生成一个新的字符串值。
> 
> 字符串会进行伸展(grown)以确保它可以将 value 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。
> 
> offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)。
>
> - 可用版本：>= 2.2.0
> - 时间复杂度: O(1)
> - 返回值：指定偏移量原来储存的位。

```
redis> SETBIT bit 10086 1
(integer) 0

redis> GETBIT bit 10086
(integer) 1

redis> GETBIT bit 100   # bit 默认被初始化为 0
(integer) 0
```
上面摘自Redis手册，可见，SETBIT方法可以针对string类型的value做bit级别的操作，而Bloom filter也是针对bit进行操作，因此我们可以利用SETBIT来实现Bloom filter。

下面我们就来基于PHP，一步一步来实现一个通用的Bloom filter。

## 基于phpredis的Demo

### BKDRHash

BKDRHash是一个即好记忆效果又很突出的哈希函数[^hashfunc]，C语言描述如下：

```
// BKDR Hash Function
unsigned int BKDRHash(char *str)
{
    unsigned int seed = 131; // 31 131 1313 13131 131313 etc..
    unsigned int hash = 0;

    while (*str)
    {
        hash = hash * seed + (*str++);
    }

    return (hash & 0x7FFFFFFF);
}
```

Bloom filter算法需要多个Hash函数，我们可以给BKRDHash设置不同的seed来完成多Hash计算，如下文PHP代码所示。

### php的BRDKHash实现

```
function getBKDRHashSeed($n) {
    if ($n === 0) return 31; 
    $j = $n + 2;
    $r = 0;
    for ($i = 0; $i < $j; $i ++) {
        if ($i % 2) {// 奇数
            $r = $r * 10 + 3;
        } else {
            $r = $r * 10 + 1;
        }   
    }   
    return $r; 
}   

function BKDRHash($str, $seed) {
    $hash = 0;
    $len = strlen($str);
    $i = 0;
    while ($i < $len) {
        $hash = ((floatval($hash * $seed) & 0x7FFFFFFF) + ord($str[$i])) & 0x7FFFFFFF;
        $i ++; 
    }   
    return ($hash & 0x7FFFFFFF);
} 
```
getBKDRHashSeed函数用来获取不同的seed，n依次从0取到k-1，从而得到k个seed，传入BKDRHash，计算出k个hashCode。

### 实现代码

```
class Bf
{

	public $redis;
	public $key;
	public $m;
	public $k;

	public function __construct($key, $m, $k) {
		if ($m > 4294967296) {
			error_log('ERROR: m over 4294967296');
			return false;
		}
		$this->key = $key;
		$this->m = $m;
		$this->k = $k;
		$this->redis = new Redis();
		$this->redis->connect('127.0.0.1', 6379);
	}

	public function add($e) {
		$e = (string)$e;
		$c = 0;
		for ($i = 0; $i < $this->k; $i ++) {
			$seed = self::getBKDRHashSeed($i);
			$hash = self::BKDRHash($e, $seed);
			$offset = $hash % $this->m;
			$t1 = microtime(true);
			$c += $this->redis->setbit($this->key, $offset, 1);
			$t2 = microtime(true);
			$cost = round(($t2-$t1)*1000, 3).'ms';
			error_log('[' . date('Y-m-d H:i:s', time()) . '] DEBUG: redis-time-spent=' . $cost . ' entry=' . $e . ' c=' . $c);
		}
		return $c === $this->k;
	}

	public function flushall() {
		return $this->redis->delete($this->key);
	}

	static public function getBKDRHashSeed($n) {
		if ($n === 0) return 31;
		$j = $n + 2;
		$r = 0;
		for ($i = 0; $i < $j; $i ++) {
			if ($i % 2) {// 奇数
				$r = $r * 10 + 3;
			} else {
				$r = $r * 10 + 1;
			}
		}
		return $r;
	}

	static public function BKDRHash($str, $seed) {
		$hash = 0;
		$len = strlen($str);
		$i = 0;
		while ($i < $len) {
			$hash = ((floatval($hash * $seed) & 0x7FFFFFFF) + ord($str[$i])) & 0x7FFFFFFF;
			$i ++;
		}
		return ($hash & 0x7FFFFFFF);
	}
}

```

上面的代码就是Bloom filter的类实现。

```
#!/usr/bin/env php 
<?php
$n = empty($_SERVER['argv'][1]) ? pow(2, 30) : intval($_SERVER['argv'][2]);
for ($i = 0; $i < $n; $i ++) {
    $word = genRandWord();
    echo $word . "\n";
}

function genRandWord() {
    $max = rand(4, 12);
    $chars = []; 
    for ($i = 0; $i < $max; $i ++) {
        $chars[] = chr(rand(97, 122));
    }   
    $word = join('', $chars);
    return $word;
}
```
为了测试，我们通过上述代码生成了1000w条随即字符串（长度[4,12]，全小写字母），写入到sample.txt文件中。

看看有多少重复的：

```
[tf@jp002 bf4redis]$ cat sample.txt |wc -l
10000000
[tf@jp002 bf4redis]$ cat sample.txt |sort |uniq |wc -l
9254122
```
重复量为：10000000 - 9254122 = 745878

测试脚本如下：

```
$fp = fopen('./sample.txt', 'r');
while ($word = fgets($fp)) {
    $word = trim($word);
    if (empty($word)) {
        continue;
    }   
    $rt = $bf->add($word);
    if ($rt) {
        error_log('WARNING: ' . $word . ' EXIST!');
    }   
}
fclose($fp);
```

测试参数：
- m=2^32=4294967296(m/n = 4294967296/10000000 ≈ 429.50)
- k=8

## 测试结果

- 总耗时：1h4m45s
- Bloom-Filter Add QPS: 2574/s
- Redis QPS：20592/s（一次add操作需要请求k（8）次redis）
- 正确性：
    ```
    [tf@jp002 bf4redis]$ cat v1.log |grep 'EXIST!' |wc -l
    745878
    ```
- 误判数：0

## 优化

上面代码中，每次往Bloom filter中add一条数据，需要请求k次redis，性能都损耗在网络IO上了，我们先将这个环节给优化掉。

### redis的pipelining介绍

Redis Pipelining可以一次发送多个命令，并按顺序执行、返回结果，节省RTT(Round Trip Time)。

每个SETBIT都是独立的，之间没有任何联系，没有必要保证其原子性，因此无需采用multi方式，距相关资料查证，采用pipelining的效率提升10倍左右，而multi反而会降低效率。

### 优化后的类

```
class Bf
{

	public $redis;
	public $key;
	public $m;
	public $k;

	public function __construct($key, $m, $k) {
		if ($m > 4294967296) {
			error_log('ERROR: m over 4294967296');
			return false;
		}
		$this->key = $key;
		$this->m = $m;
		$this->k = $k;
		$this->redis = new Redis();
		$this->redis->connect('127.0.0.1', 6379);
	}

	public function add($e) {
		$e = (string)$e;
		$this->redis->multi(Redis::PIPELINE);
		for ($i = 0; $i < $this->k; $i ++) {
			$seed = self::getBKDRHashSeed($i);
			$hash = self::BKDRHash($e, $seed);
			$offset = $hash % $this->m;
			$this->redis->setbit($this->key, $offset, 1);
		}
		$t1 = microtime(true);
		$rt = $this->redis->exec();
		$t2 = microtime(true);
		$cost = round(($t2-$t1)*1000, 3).'ms';
		$c = array_sum($rt);
		error_log('[' . date('Y-m-d H:i:s', time()) . '] DEBUG: redis-time-spent=' . $cost . ' entry=' . $e . ' c=' . $c);
		return $c === $this->k;
	}

	public function flushall() {
		return $this->redis->delete($this->key);
	}

	static public function getBKDRHashSeed($n) {
		if ($n === 0) return 31;
		$j = $n + 2;
		$r = 0;
		for ($i = 0; $i < $j; $i ++) {
			if ($i % 2) {// 濂囨暟
				$r = $r * 10 + 3;
			} else {
				$r = $r * 10 + 1;
			}
		}
		return $r;
	}

	static public function BKDRHash($str, $seed) {
		$hash = 0;
		$len = strlen($str);
		$i = 0;
		while ($i < $len) {
			$hash = ((floatval($hash * $seed) & 0x7FFFFFFF) + ord($str[$i])) & 0x7FFFFFFF;
			$i ++;
		}
		return ($hash & 0x7FFFFFFF);
	}
}

```

## 优化后的测试结果

- 总耗时：13m21s
- Bloom-Filter Add QPS: 12000/s
- Redis QPS：12000/s
- 正确性：
    ```
    [tf@jp002 bf4redis]$ cat v2.log |grep 'EXIST!' |wc -l
    745878
    ```
- 误判数：0

速度提升了5倍！

## 再优化

刚刚Redis官方文档里面对SETBIT的介绍中有这样一句：
> **bit 映射被限制在 512 MB 之内**

往回翻看上文中
> False-Positive-Ratio表（含内存空间占用）

可以看到如果m为500亿，Bloom filter的内存空间会占用大约5.82GB，大大查过Redis的bit映射范围限制。

因此我们需要对该Bloom filter实现做分布式改造，根据m的规模， 构建多个bit表，不同的输入会sharding到对应的bit表。

### 分布式Bloom-Filter

考虑到单个redis实例的内存是有上限的，我们可以设计两级sharding：
1. 第一级将不同的输入sharding到对应的redis实例
2. 第二级将输入sharding到对应的key上（不同的key代表不同的Bloom filter）

### 优化后的demo（完整代码）

```
class Bf
{

	public $key;
	public $m;
	public $k;
	public $nPartitions;
	public $redisCfg;
	public $nRedis;

	public $maxOffs = [];

	const MAX_PARTITION_SIZE = 4294967296;	//redis string's max len is pow(2, 32) bits = 512MB
	//const MAX_PARTITION_SIZE = 65536;

	public function __construct($redisCfg, $key, $m, $k) {
		$this->nRedis = count($redisCfg);
		if ($m > self::MAX_PARTITION_SIZE) {
			$this->nPartitions = ceil(ceil($m / $this->nRedis) / self::MAX_PARTITION_SIZE);
		} else {
			$this->nPartitions = 1;
		}
		$this->key = $key;
		$this->m = $m;
		$this->k = $k;
		$this->redisCfg = $redisCfg;
	}

	private function getPosition($e) {
		$nRedis = count($this->redisCfg);
		$hash = crc32($e);
		$i = $hash % $nRedis;
		$redis = SRedis::getSingeton($this->redisCfg[$i]);
		$key = $this->key . '.' . $hash % $this->nPartitions;
		return [$i, $redis, $key];
	}

	public function add($e) {
		$e = (string)$e;
		list($n, $redis, $key) = $this->getPosition($e);
		//var_dump($this->key, $this->m, $this->k, $this->nRedis, $this->nPartitions, $redis, $key);
		$redis->multi(Redis::PIPELINE);
		for ($i = 0; $i < $this->k; $i ++) {
			$seed = self::getBKDRHashSeed($i);
			$hash = self::BKDRHash($e, $seed);
			$offset = $hash % $this->m;
			if ($offset > @$this->maxOffs[$n.'|'.$key]) $this->maxOffs[$n.'|'.$key] = $offset;	//only 4 log
			$redis->setbit($key, $offset, 1);
		}
		$t1 = microtime(true);
		$rt = $redis->exec();
		$t2 = microtime(true);
		$cost = round(($t2-$t1)*1000, 3).'ms';
		$c = array_sum($rt);
		error_log('[' . date('Y-m-d H:i:s', time()) . '] DEBUG: redis[' . $n . ']-time-spent=' . $cost . ' maxOffset-of-' . $n.'|'.$key . '=' . $this->maxOffs[$n.'|'.$key] . ' entry=' . $e . ' c=' . $c);
		return $c === $this->k;
	}

	public function flushall() {
		foreach ($this->redisCfg as $cfg) {
			$redis = SRedis::getSingeton($cfg);
			for ($i = 0; $i < $this->nPartitions; $i ++) {
				$redis->delete($this->key . '.' . $i);
			}
		}
	}

	static public function getBKDRHashSeed($n) {
		if ($n === 0) return 31;
		$j = $n + 2;
		$r = 0;
		for ($i = 0; $i < $j; $i ++) {
			if ($i % 2) {// 濂囨暟
				$r = $r * 10 + 3;
			} else {
				$r = $r * 10 + 1;
			}
		}
		return $r;
	}

	static public function BKDRHash($str, $seed) {
		$hash = 0;
		$len = strlen($str);
		$i = 0;
		while ($i < $len) {
			$hash = ((floatval($hash * $seed) & 0x7FFFFFFF) + ord($str[$i])) & 0x7FFFFFFF;
			$i ++;
		}
		return ($hash & 0x7FFFFFFF);
	}
}

class SRedis
{
	public function getSingeton($cfg) {
		static $pool;
		if (empty($cfg) || !is_array($cfg)) {
			return false;
		}
		$k = serialize($cfg);
		if (empty($pool[$k])) {
			$redis = new Redis();
			call_user_func_array([$redis, 'connect'], array_values($cfg));
			$pool[$k] = $redis;
		}
		return $pool[$k];
	}
}

if ($_SERVER['argc'] < 4) {
	die("Usage: ./" . $_SERVER['argv'][0] . " <bloom-filter's name> <m> <k>\n");
}
$key = trim($_SERVER['argv'][3]);
$m = intval($_SERVER['argv'][4]);
$k = intval($_SERVER['argv'][5]);

$sampleFile = __DIR__ . '/sample.txt';

$redisCfg = [
	[
		'host'				=> '127.0.0.1',
		'port'				=> 6379,
		/*
		'timeout'			=> 5,
		'reserved'			=> null,
		'retry_interval'	=> 1000,
		'read_timeout'		=> 1,
		*/
	],
];

$bf = new Bf($redisCfg, $key, $m, $k);
$bf->flushall();

$fp = fopen($sampleFile, 'r');
while ($word = fgets($fp)) {
	$word = trim($word);
	if (empty($word)) {
		continue;
	}
	$rt = $bf->add($word);
	if ($rt) {
		error_log('WARNING: ' . $word . ' EXIST!');
	}
}
fclose($fp);

```

综上，我们实现了一个基于Redis的通用Bloom filter。

## 项目repository
Github: [https://github.com/0x5446/bloomfilter-redis](https://github.com/0x5446/bloomfilter-redis)

## 参考

[^sxzm]: 吴军 [数学之美系列二十一 － 布隆过滤器（Bloom Filter）][1] 

[^bfmath]: Pei Cao [Bloom Filters - the math][2]

[^hashfunc]: BYVoid [各种字符串Hash函数比较][3]


  [1]: https://china.googleblog.com/2007/07/bloom-filter_7469.html
  [2]: http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
  [3]: https://www.byvoid.com/zhs/blog/string-hash-compare

#!/usr/bin/env php
<?php
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
$key = trim($_SERVER['argv'][1]);
$m = intval($_SERVER['argv'][2]);
$k = intval($_SERVER['argv'][3]);

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

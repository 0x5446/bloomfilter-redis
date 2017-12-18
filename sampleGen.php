#!/usr/bin/env php
<?php
$n = empty($_SERVER['argv'][1]) ? pow(2, 30) : intval($_SERVER['argv'][1]);
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

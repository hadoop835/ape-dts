-- APPEND
SET 1-1 val_0
APPEND 1-1 append_0

-- BITFIELD
-- SET
BITFIELD 2-1 SET i8 #0 100 SET i8 #1 200
-- INCRBY
BITFIELD 2-2 incrby i5 100 1
BITFIELD 2-3 incrby i5 100 1 GET u4 0
-- OVERFLOW
BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
BITFIELD 2-4 incrby u2 100 1 OVERFLOW SAT incrby u2 102 1
BITFIELD 2-4 OVERFLOW FAIL incrby u2 102 1

-- BITOP
-- AND 
SET 3-1 "foobar"
SET 3-2 "abcdef"
BITOP AND 3-3 3-1 3-2
-- OR
BITOP OR 3-4 3-1 3-2
-- XOR
BITOP XOR 3-5 3-1 3-2
-- NOT
BITOP NOT 3-6 3-1

-- BLMOVE -- version: 6.2.0
-- RPUSH 4-1 a b c
-- RPUSH 4-2 x y z
-- BLMOVE 4-1 4-2 LEFT LEFT 0

-- BLMPOP -- version: 7.0.0
-- BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
-- LPUSH 5-1 a b c d
-- LPUSH 5-2 1 2 3 4
-- BLMPOP 0 2 5-1 5-2 LEFT COUNT 3

-- BLPOP
RPUSH 6-1 a b c
BLPOP 6-1 0
-- LRANGE 6-1 0 -1

-- BRPOP
RPUSH 7-1 a b c
BRPOP 7-1 0
-- LRANGE 7-1 0 -1

-- BRPOPLPUSH
RPUSH 8-1 a b c
BRPOPLPUSH 8-1 19 0

-- BZMPOP -- version: 7.0.0
-- ZADD 9-1 1 a 2 b 3 c
-- ZADD 9-2 1 d 2 e 3 f
-- BZMPOP 1 2 9-1 9-2 MIN
-- ZRANGE 9-2 0 -1 WITHSCORES

-- BZPOPMAX -- version: 5.0.0
-- ZADD 10-1 0 a 1 b 2 c
-- BZPOPMAX 10-1 23 0

-- BZPOPMIN -- version: 5.0.0
-- ZADD 11-1 0 a 1 b 2 c
-- BZPOPMIN 11-1 25 0
-- ZRANGE 11-1 0 -1 WITHSCORES

-- COPY -- version: 6.2.0
-- SET 12-1 "sheep"
-- COPY 12-1 12-2
-- GET 12-2

-- DECR
SET 13-1 "10"
DECR 13-1

-- DECRBY
SET 14-1 "10"
DECRBY 14-1 3

-- EXPIRE
SET 15-1 "Hello"
EXPIRE 15-1 1
-- Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
-- EXPIRE 15-1 1 XX
-- EXPIRE 15-1 1 NX
SET 15-2 "Hello"
-- NOT expire during test
EXPIRE 15-2 1000000000

-- EXPIREAT
SET 16-1 "Hello"
EXPIREAT 16-1 1
SET 16-2 "Hello"
-- NOT expire during test
EXPIREAT 16-2 4102416000

-- GEOADD
GEOADD 17-1 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
-- GEODIST 17-1 Palermo Catania

-- GETDEL -- version: 6.2.0
-- SET 18-1 "Hello"
-- GETDEL 18-1

-- GETEX -- version: 6.2.0
-- SET 19-1 "Hello"
-- GETEX 19-1 EX 1

-- GETSET -- version: 6.2.0
-- SET 20-1 "Hello"
-- GETSET 20-1 "World"

-- HSET
HSET 21-1 field1 "hello" field2 "world"

-- HINCRBY
HSET 22-1 field 5
HINCRBY 22-1 field 1
HINCRBY 22-1 field -2

-- HINCRBYFLOAT
HSET 23-1 field_1 10.50
HINCRBYFLOAT 23-1 field_1 0.1
HINCRBYFLOAT 23-1 field_2 -5

-- HMSET
HMSET 24-1 field1 "Hello" field2 "World"

-- HSET
HSET 24-1 field2 "Hi" field3 "World"

-- HSETNX
HSETNX 25-1 field "Hello"
HSETNX 25-1 field "World"

-- INCR
SET 26-1 "10"
INCR 26-1

-- INCRBY
SET 27-1 "10"
INCRBY 27-1 5

-- INCRBYFLOAT
SET 28-1 10.50
INCRBYFLOAT 28-1 0.1
INCRBYFLOAT 28-1 -5

-- LINSERT
RPUSH 29-1 "Hello"
RPUSH 29-1 "World"
LINSERT 29-1 BEFORE "World" "There"
-- LRANGE 29-1 0 -1

-- LMOVE --version: 6.2.0
-- RPUSH 30-1 "one"
-- RPUSH 30-1 "two"
-- RPUSH 30-1 "three"
-- LMOVE 30-1 30-2 RIGHT LEFT
-- LMOVE 30-1 30-2 LEFT RIGHT
-- LRANGE 30-1 0 -1
-- LRANGE 30-2 0 -1

-- LMPOP --version: 7.0.0
-- LPUSH 31-1 "one" "two" "three" "four" "five"
-- LMPOP 1 31-1 LEFT
-- LRANGE 31-1 0 -1
-- LMPOP 1 31-1 RIGHT COUNT 10

-- LPOP 
RPUSH 32-1 "one" "two" "three" "four" "five"
LPOP 32-1
-- Starting with Redis version 6.2.0: Added the count argument.
-- LPOP 32-1 2 
-- LRANGE 32-1 0 -1

-- LPUSH
LPUSH 33-1 "world"
LPUSH 33-1 "hello"
-- LRANGE 33-1 0 -1

-- LPUSHX
LPUSH 34-1 "World"
LPUSHX 34-1 "Hello"
LPUSHX 34-2 "Hello"
-- LRANGE 34-1 0 -1
-- LRANGE 34-2 0 -1

-- LREM
RPUSH 35-1 "hello"
RPUSH 35-1 "hello"
RPUSH 35-1 "foo"
RPUSH 35-1 "hello"
LREM 35-1 -2 "hello"
-- LRANGE 35-1 0 -1

-- LSET
RPUSH 36-1 "one"
RPUSH 36-1 "two"
RPUSH 36-1 "three"
LSET 36-1 0 "four"
LSET 36-1 -2 "five"
-- LRANGE 36-1 0 -1

-- LTRIM
RPUSH 37-1 "one"
RPUSH 37-1 "two"
RPUSH 37-1 "three"
LTRIM 37-1 1 -1
-- LRANGE 37-1 0 -1

-- MOVE
SET 38-1 1
MOVE 38-1 1

-- MSET
MSET 39-1 "Hello" 39-2 "World"

-- MSETNX
MSETNX 40-1 "Hello" 40-2 "there"
MSETNX 40-2 "new" 40-3 "world"
MGET 40-1 40-2 40-3

-- PERSIST
SET 41-1 "Hello"
EXPIRE 41-1 10000000
PERSIST 41-1

-- PEXPIRE
SET 42-1 "Hello"
-- NOT expire during test
PEXPIRE 42-1 1500000000
-- Starting with Redis version 7.0.0: Added options: NX, XX, GT and LT.
-- SET 42-2 "Hello"
-- PEXPIRE 42-2 1000 XX
-- SET 42-3 "Hello"
-- PEXPIRE 42-3 1000 NX

-- PEXPIREAT
SET 43-1 "Hello"
PEXPIREAT 43-1 1555555555005
SET 43-2 "Hello"
-- NOT expire during test
PEXPIREAT 43-2 15555555550050000
-- PEXPIRETIME 43-1

-- PFADD
PFADD 44-1 a b c d e f g
-- PFCOUNT 44-1
-- GET 44-1

-- PFMERGE
PFADD 45-1 foo bar zap a
PFADD 45-2 a b c foo
PFMERGE 45-3 45-1 45-2
-- PFCOUNT 45-3
-- GET 45-3

-- PSETEX (deprecated)
PSETEX 46-1 1000 "Hello"
-- PTTL 46-1
-- NOT expire during test
PSETEX 46-2 100000000 "Hello"
-- GET 46-2

-- RENAME
SET 47-1 "Hello"
RENAME 47-1 47-2
GET 47-2

-- RENAMENX
SET 48-1 "Hello"
SET 48-2 "World"
RENAMENX 48-1 48-2
-- GET 48-2

-- RPOP
RPUSH 49-1 "one" "two" "three" "four" "five"
RPOP 49-1
-- Starting with Redis version 6.2.0: Added the count argument.
-- RPOP 49-1 2
-- LRANGE 49-1 0 -1

-- RPOPLPUSH (deprecated)
RPUSH 50-1 "one"
RPUSH 50-1 "two"
RPUSH 50-1 "three"
RPOPLPUSH 50-1 50-2
-- LRANGE 50-1 0 -1
-- LRANGE 50-2 0 -1

-- RPUSH
RPUSH 51-1 "hello"
RPUSH 51-1 "world"
-- LRANGE 51-1 0 -1

-- RPUSHX
RPUSH 52-1 "Hello"
RPUSHX 52-1 "World"
RPUSHX 52-2 "World"
-- LRANGE 52-1 0 -1
-- LRANGE 52-2 0 -1

-- SADD
SADD 53-1 "Hello"
SADD 53-1 "World"
SADD 53-1 "World"
SADD 53-2 1000
SADD 53-2 2000
SADD 53-2 3000
-- SMEMBERS 53-1
-- SORT 53-1 ALPHA

-- SDIFFSTORE
SADD 54-1 "a"
SADD 54-1 "b"
SADD 54-1 "c"
SADD 54-2 "c"
SADD 54-2 "d"
SADD 54-2 "e"
SDIFFSTORE 54-3 54-1 54-2
-- SMEMBERS 54-3
-- SORT 54-3 ALPHA

-- SETBIT
SETBIT 55-1 7 1
SETBIT 55-1 7 0
-- GET 55-1

-- SETEX
SETEX 56-1 1 "Hello"
-- GET 56-1
-- NOT expire during test
SETEX 56-2 100000000 "Hello"

-- SETNX
SETNX 57-1 "Hello"
SETNX 57-1 "World"
-- GET 57-1

-- SETRANGE
SET 58-1 "Hello World"
SETRANGE 58-1 6 "Redis"
-- GET 58-1
SETRANGE 58-2 6 "Redis"
-- GET 58-2

-- SINTERSTORE
SADD 59-1 "a"
SADD 59-1 "b"
SADD 59-1 "c"
SADD 59-2 "c"
SADD 59-2 "d"
SADD 59-2 "e"
SINTERSTORE 59-3 59-1 59-2
-- SMEMBERS 59-3

-- SMOVE
SADD 60-1 "one"
SADD 60-1 "two"
SADD 60-2 "three"
SMOVE 60-1 60-2 "two"
-- SMEMBERS 60-1
-- SMEMBERS 60-2

-- SPOP
SADD 61-1 "one"
SADD 61-1 "two"
SADD 61-1 "three"
SPOP 61-1
-- SMEMBERS 61-1
SADD 61-1 "four"
SADD 61-1 "five"
SPOP 61-1 3
-- SMEMBERS 61-1

-- SREM
SADD 62-1 "one"
SADD 62-1 "two"
SADD 62-1 "three"
SREM 62-1 "one"
SREM 62-1 "four"
-- SMEMBERS 62-1

-- SUNIONSTORE
SADD 63-1 "a"
SADD 63-2 "b"
SUNIONSTORE key 63-1 63-2
-- SMEMBERS key

-- SWAPDB
SWAPDB 0 1

-- UNLINK
SET 64-1 "Hello"
SET 64-2 "World"
UNLINK 64-1 64-2 64-3

-- -- XACK
-- XADD mystream1 1526569495631-0 message "Hello,"
-- XACK mystream1 mygroup 1526569495631-0
-- -- XRANGE mystream1 - +

-- XADD -- version: 5.0.0
-- XADD 65-1 1526919030474-55 message "Hello,"
-- -- Starting with Redis version 7.0.0: Added support for the <ms>-* explicit ID form.
-- -- XADD 65-1 1526919030474-* message " World!"
-- XADD 65-1 * name Sara surname OConnor
-- XADD 65-1 * field1 value1 field2 value2 field3 value3
-- -- XLEN 65-1
-- -- XRANGE 65-1 - +

-- -- XAUTOCLAIM
-- XAUTOCLAIM mystream mygroup Alice 3600000 0-0 COUNT 25

-- -- XCLAIM
-- XCLAIM mystream mygroup Alice 3600000 1526569498055-0

-- XDEL -- version: 5.0.0
-- XADD 66-1 1538561700640-0 a 1
-- XADD 66-1 * b 2
-- XADD 66-1 * c 3
-- XDEL 66-1 1538561700640-0
-- XRANGE 66-1 - +

-- XGROUP CREATE mystream mygroup 0

-- XTRIM -- version: 5.0.0
-- XTRIM 67-1 MAXLEN 1000
-- XADD 67-1 * field1 A field2 B field3 C field4 D
-- XTRIM 67-1 MAXLEN 2
-- XRANGE 67-1 - +

-- ZADD
ZADD 68-1 1 "one"
ZADD 68-1 1 "uno"
ZADD 68-1 2 "two" 3 "three"
-- ZRANGE 68-1 0 -1 WITHSCORES

-- ZDIFFSTORE -- version: 6.2.0
-- ZADD 69-1 1 "one"
-- ZADD 69-1 2 "two"
-- ZADD 69-1 3 "three"
-- ZADD 69-2 1 "one"
-- ZADD 69-2 2 "two"
-- ZDIFFSTORE 69-3 2 69-1 69-2
-- ZRANGE 69-3 0 -1 WITHSCORES

-- ZINCRBY
ZADD 70-1 1 "one"
ZADD 70-1 2 "two"
ZINCRBY 70-1 2 "one"
-- ZRANGE 70-1 0 -1 WITHSCORES

-- ZINTERSTORE
ZADD 71-1 1 "one"
ZADD 71-1 2 "two"
ZADD 71-2 1 "one"
ZADD 71-2 2 "two"
ZADD 71-2 3 "three"
ZINTERSTORE 71-3 2 71-1 71-2 WEIGHTS 2 3
-- ZRANGE 71-3 0 -1 WITHSCORES

-- ZMPOP -- version: 7.0.0
-- ZADD 72-1 1 "one" 2 "two" 3 "three"
-- ZMPOP 1 72-1 MIN
-- ZRANGE 72-1 0 -1 WITHSCORES

-- ZPOPMAX -- version: 5.0.0
-- ZADD 73-1 1 "one"
-- ZADD 73-1 2 "two"
-- ZADD 73-1 3 "three"
-- ZPOPMAX 73-1

-- ZPOPMIN -- version: 5.0.0
-- ZADD 74-1 1 "one"
-- ZADD 74-1 2 "two"
-- ZADD 74-1 3 "three"
-- ZPOPMIN 74-1

-- ZRANGESTORE -- version: 6.2.0
-- ZADD 75-1 1 "one" 2 "two" 3 "three" 4 "four"
-- ZRANGESTORE 75-2 75-1 2 -1
-- ZRANGE 75-2 0 -1

-- ZREM
ZADD 76-1 1 "one"
ZADD 76-1 2 "two"
ZADD 76-1 3 "three"
ZREM 76-1 "two"
-- ZRANGE 76-1 0 -1 WITHSCORES

-- ZREMRANGEBYLEX
ZADD 77-1 0 aaaa 0 b 0 c 0 d 0 e
ZADD 77-1 0 foo 0 zap 0 zip 0 ALPHA 0 alpha
ZREMRANGEBYLEX 77-1 [alpha [omega
ZRANGE 77-1 0 -1

-- ZREMRANGEBYRANK
ZADD 78-1 1 "one"
ZADD 78-1 2 "two"
ZADD 78-1 3 "three"
ZREMRANGEBYRANK 78-1 0 1
-- ZRANGE 78-1 0 -1 WITHSCORES

-- ZREMRANGEBYSCORE
ZADD 79-1 1 "one"
ZADD 79-1 2 "two"
ZADD 79-1 3 "three"
ZREMRANGEBYSCORE 79-1 -inf (2
-- ZRANGE 79-1 0 -1 WITHSCORES

-- ZUNIONSTORE
ZADD 80-1 1 "one"
ZADD 80-1 2 "two"
ZADD 80-2 1 "one"
ZADD 80-2 2 "two"
ZADD 80-2 3 "three"
ZUNIONSTORE 80-3 2 80-1 zset2 WEIGHTS 2 3
-- ZRANGE 80-3 0 -1 WITHSCORES
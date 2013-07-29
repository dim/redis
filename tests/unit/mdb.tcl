start_server {tags {"mdb"} overrides {mdbenabled yes}} {

    test {MMFLUSHDB} {
        r mmset x 1
        assert {[r mmdbsize] eq {1}}
        r mmflushdb
        r mmdbsize
    } {0}

    test {MMSET an item} {
        r mmset x foobar
    } {OK}

    test {MMGET an item} {
        r mmset x foobar
        r mmget x
    } {foobar}

    test {MMSET sequential} {
        r mmset x foo
        r mmset y bar

        set v1 [r mmget x]
        set v2 [r mmget y]
        list $v1 $v2
    } {foo bar}

    test {MMSET MMGET an empty item} {
        r mmset x {}
        r mmget x
    } {}

    test {Very big payload in MMGET/MMSET} {
        set buf [string repeat "abcd" 100000]
        r mmset foo $buf
        r mmget foo
    } [string repeat "abcd" 100000]

    test {MMSET key too long} {
        set buf [string repeat "a" 1024]
        catch {r mmset $buf foo} err
        format $err
    } {ERR*}

    test {Extended MMSET EX option} {
        r mmflushdb
        r mmset foo bar ex 10
        set ttl [r mmttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended MMSET PX option} {
        r mmflushdb
        r mmset foo bar px 10000
        set ttl [r mmttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended MMSET NX option} {
        r mmflushdb
        set v1 [r mmset foo 1 nx]
        set v2 [r mmset foo 2 nx]
        list $v1 $v2 [r mmget foo]
    } {OK {} 1}

    test {Extended MMSET XX option} {
        r mmflushdb
        set v1 [r mmset foo 1 xx]
        r mmset foo bar
        set v2 [r mmset foo 2 xx]
        list $v1 $v2 [r mmget foo]
    } {{} OK 2}

    test {MMSET replace} {
        r mmflushdb
        r mmset foo x
        r mmset foo y
        r mminfo
    } {*main:1*}

    test {MMDEL against a single item} {
        r mmdel x
        r mmget x
    } {}

    test {MMDEL against a multiple items} {
        r mmset x 1
        r mmset y 2
        r mmdel x y

        set res {}
        append res [r mmget x]
        append res [r mmget y]
    } {}

    test {MMKEYS with pattern} {
        r mmflushdb
        foreach key {key_x key_y key_z foo_a foo_b foo_c} {
            r mmset $key hello
        }
        lsort [r mmkeys foo*]
    } {foo_a foo_b foo_c}

    test {MMKEYS to get all keys} {
        lsort [r mmkeys *]
    } {foo_a foo_b foo_c key_x key_y key_z}

    test {MMINCRBY against non existing key} {
        set res {}
        append res [r mmincrby novar 1]
        append res [r mmget novar]
    } {11}

    test {MMINCRBY against key created by incr itself} {
        r mmincrby novar 1
    } {2}

    test {MMINCRBY against key originally set with MMSET} {
        r mmset novar 100
        r mmincrby novar 1
    } {101}

    test {MMINCRBY over 32bit value} {
        r mmset novar 17179869184
        r mmincrby novar 1
    } {17179869185}

    test {MMINCRBY over 32bit value with over 32bit increment} {
        r mmset novar 17179869184
        r mmincrby novar 17179869184
    } {34359738368}

    test {MMINCRBY fails against key with spaces (left)} {
        r mmset novar "    11"
        catch {r mmincrby novar 1} err
        format $err
    } {ERR*}

    test {MMINCRBY fails against key with spaces (right)} {
        r mmset novar "11    "
        catch {r mmincrby novar 1} err
        format $err
    } {ERR*}

    test {MMINCRBY fails against key with spaces (both)} {
        r mmset novar "    11    "
        catch {r mmincrby novar 1} err
        format $err
    } {ERR*}

    test {MMINCRBY retains ttl} {
        r mmset foo 1 ex 60
        r mmincrby foo 1
        set ttl [r mmttl foo]
        assert {$ttl <= 60 && $ttl > 0}
    }

    test {MMTYPE against a non-exiting} {
        r mmdel x
        r mmtype x
    } {none}

    test {MMTYPE against a string} {
        r mmset x foobar
        r mmtype x
    } {string}

    test {MMTYPE against an empty item} {
        r mmset x {}
        r mmtype x
    } {string}

    test {MMSTRLEN against a string} {
        r mmset x foobar
        r mmstrlen x
    } {6}

    test {MMSTRLEN against an integer} {
        r mmset x 1233
        r mmincrby x 1
        r mmstrlen x
    } {4}

    test {MMSTRLEN against an empty item} {
        r mmset x {}
        r mmstrlen x
    } {0}

    test {MMSTRLEN against an non-existing key} {
        r mmdel x
        r mmstrlen x
    } {0}

    test {MMSTRLEN against a long string} {
        set buf [string repeat "abcd" 10000]
        r mmset x $buf
        r mmstrlen x
    } {40000}

    test {MMEXISTS basics} {
        set res {}
        r mmset newkey test
        append res [r mmexists newkey]
        r mmdel newkey
        append res [r mmexists newkey]
    } {10}

    test {Zero length value in key. SET/GET/EXISTS} {
        r mmset emptykey {}
        set res [r mmget emptykey]
        append res [r mmexists emptykey]
        r mmdel emptykey
        append res [r mmexists emptykey]
    } {10}

    test {MMEXPIRE - set timeouts multiple times} {
        r mmset x foobar
        set v1 [r mmexpire x 5]
        set v2 [r mmttl x]
        set v3 [r mmexpire x 10]
        set v4 [r mmttl x]
        set v5 [r mmget x]
        list $v1 $v2 $v3 $v4 $v5
    } {1 [45] 1 10 foobar}

    test {MMDEL - removes TTL} {
        r mmexpire x 60
        r mmdel x
        r mmset x somevalue
        r mmttl x
    } {-1}

    test {PTTL returns millisecond time to live} {
        r mmdel x
        r mmset x foobar
        r mmexpire x 1
        set ttl [r mmpttl x]
        assert {$ttl > 900 && $ttl <= 1000}
    }

    test {MMGET/MMSET/MMINCRBY scripting } {
        r mmset x foobar
        r eval {
            local get = redis.call('mmget','x')
            local set = redis.call('mmset','x', '5')
            local inc = redis.call('mmincrby','x', '11')
            local fin = redis.call('mmget','x')
            return {get,set,inc,fin}
        } 0
    } {foobar OK 16 16}

    test {MMDEBUG OBJECT no key} {
        r mmdel x
        catch {r mmdebug object x} err
        format $err
    } {ERR*}

    test {MMDEBUG OBJECT empty key} {
        r mmset x {}
        r mmdebug object x
    } {Value type:string encoding:raw length:0 expiration:-1}

    test {MMDEBUG OBJECT string key} {
        r mmset x foo
        r mmdebug object x
    } {Value type:string encoding:raw length:3 expiration:-1}

    test {MMDEBUG OBJECT with expiration} {
        r mmexpireat x 1818181818
        r mmdebug object x
    } {Value type:string encoding:raw length:3 expiration:1818181818000}

    test {MMDEBUG OBJECT numeric} {
        r mmset x 1200
        r mmincrby x 34
        r mmdebug object x
    } {Value type:string encoding:raw length:4 expiration:-1}

    test {MMINFO} {
        r mmflushdb
        r mmset x foo
        r mmset y bar
        r mminfo
    } {MDBInfo mapsize:33554431 readers:?/* main:2}

    test {MMAPPEND basics} {
        r mmdel foo
        list [r mmappend foo bar] [r mmget foo] \
             [r mmappend foo 100] [r mmget foo]
    } {3 bar 6 bar100}

    test {MMAPPEND retains ttl} {
        r mmset foo bar ex 60
        r mmappend foo bar
        set ttl [r mmttl foo]
        assert {$ttl <= 60 && $ttl > 0}
    }

    test {GETRANGE against non-existing key} {
        r mmdel foo
        assert_equal "" [r mmgetrange foo 0 -1]
    }

    test {GETRANGE against string value} {
        r mmset foo "Hello World"
        assert_equal "Hell" [r mmgetrange foo 0 3]
        assert_equal "Hello World" [r mmgetrange foo 0 -1]
        assert_equal "orld" [r mmgetrange foo -4 -1]
        assert_equal "" [r mmgetrange foo 5 3]
        assert_equal " World" [r mmgetrange foo 5 5000]
        assert_equal "Hello World" [r mmgetrange foo -5000 10000]
    }

    if {!$::valgrind} {
        test {MMGET/MMSET/MMINCRBY after BGSAVE } {
            r mmset x foobar
            waitForBgsave r
            r bgsave
            waitForBgsave r
            list [r mmget x] [r mmset x 5] \
                 [r mmincrby x 11] [r mmget x]
        } {foobar OK 16 16}
    }

    test {Active expiration should perform in background} {
        r mmflushdb

        set k {a}
        for {set i 0} {$i < 10} {incr i} {
            r mmset [append k $i] val
        }
        r mmset b9 val px 100
        wait_for_condition 50 100 {
            [r mmdbsize] eq {10}
        } else {
            fail "Failed to expire"
        }
    }

    test {Active expiration should cover all keys} {
        r mmflushdb

        set k {b}
        for {set i 0} {$i < 10} {incr i} {
            r mmset [append k $i] val
        }
        r mmset a0 val px 100
        wait_for_condition 50 100 {
            [r mmdbsize] eq {10}
        } else {
            fail "Failed to expire"
        }
    }
}

start_server {tags {mdb disabled}} {

    test {refuses MMFLUSHDB} {
        catch {r mmflushdb} err
        format $err
    } {ERR unknown command 'mmflushdb'}

    test {refuses MMSET} {
        catch {r mmset x foobar} err
        format $err
    } {ERR unknown command 'mmset'}

    test {refuses MMGET} {
        catch {r mmget x} err
        format $err
    } {ERR unknown command 'mmget'}
}
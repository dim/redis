start_server {tags {"mdb repl"} overrides {mdbenabled yes}} {
    r mmset foo x
    r mmset bar y
    r mmset baz z
    r mmexpire bar 3600

    start_server {overrides {mdbenabled yes}} {

        test {Second server should have role master at first} {
            s 0 role
        } {master}

        test {Second server should not have any keys} {
            r 0 mmdbsize
        } {0}

        test {SLAVEOF should start with link status "down"} {
            r slaveof [srv -1 host] [srv -1 port]
            s master_link_status
        } {down}

        test {The role should immediately be changed to "slave"} {
            s role
        } {slave}

        test {The link status should be up} {
            wait_for_condition 200 50 {
                [s master_link_status] eq {up}
            } else {
                fail "Failed to sync"
            }
        }

        test {Sync should have transferred keys from master} {
            r mmkeys *
        } {bar baz foo}

        test {Sync should retain expiration information} {
            set ttl [r mmttl bar]
            assert {$ttl <= 3600 && $ttl > 3590}
        }

        test {MMSET on the master should immediately propagate} {
            r -1 mmset foo bar
            wait_for_condition 100 50 {
                [r 0 mmget foo] eq {bar}
            } else {
                fail "MMSET on master did not propagated on slave"
            }
        }

        test {MMEXPIRE should propagate} {
            r -1 mmexpire foo 0
            wait_for_condition 100 50  {
                [r -1 mmexists foo] eq {0}
            } else {
                fail "MMEXPIRE failed on master"
            }
            wait_for_condition 100 50  {
                [r 0 mmexists foo] eq {0}
            } else {
                fail "MMEXPIRE failed to propagate"
            }
        }

        test {MMFLUSHDB should propagate} {
            r -1 mmflushdb
            assert {[r -1 mmdbsize] eq {0}}
            wait_for_condition 100 50  {
                [r 0 mmdbsize] eq {0}
            } else {
                fail "MMFLUSHDB failed to propagate"
            }
        }

    }
}

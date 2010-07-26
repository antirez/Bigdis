# BIGDIS -- A filesystem based DB speaking the Redis protocol for large files.
#
# Copyright (c) 2006-2009, Salvatore Sanfilippo
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Redis nor the names of its contributors may be
#      used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

array set ::clients {}
array set ::mbulk {}
array set ::argvector {}
array set ::argcount {}
array set ::readlen {}
array set ::bulkfile {}
array set ::bulkfd {}
array set ::replyqueue {}
set ::listensocket {}
set ::tmpfileid 0

source bigdis.conf

package require sha1 2.0.0

################################################################################
# Utility functions
################################################################################

proc sha1_hex str {
    ::sha1::sha1 -hex $str
}

proc log msg {
    puts stderr "[clock format [clock seconds]]\] $msg "
}

proc warning msg {
    log "*** WARNING: $msg"
}

################################################################################
# Networking
################################################################################

proc update_last_interaction {fd} {
    set ::clients($fd) [clock seconds]
}

proc reset_client {fd} {
    if {![info exists ::clients($fd)]} return
    update_last_interaction $fd
    set ::argvector($fd) {}
    set ::argcount($fd) 0
    set ::readlen($fd) -1
    set ::mbulk($fd) -1
    if {$::bulkfd($fd) ne {}} {
        close $::bulkfd($fd)
    }
    if {$::bulkfile($fd) ne {}} {
        file delete -- $::bulkfile($fd)
    }
    set ::bulkfile($fd) {}
    set ::bulkfd($fd) {}
}

proc init_client {fd} {
    update_last_interaction $fd
    set ::mbulk($fd) -1
    set ::argvector($fd) {}
    set ::argcount($fd) 0
    set ::readlen($fd) -1
    set ::bulkfile($fd) {}
    set ::bulkfd($fd) {}
    set ::replyqueue($fd) {}
}

proc close_client fd {
    reset_client $fd
    unset ::clients($fd)
    unset ::argvector($fd)
    unset ::readlen($fd)
    unset ::bulkfile($fd)
    unset ::bulkfd($fd)
    unset ::mbulk($fd)
    unset ::argcount($fd)
    for {set j 0} {$j < [llength $::replyqueue($fd)]} {incr j} {
        set item [lindex $::replyqueue($fd)]
        if {[lindex $item 0] eq {file}} {
            close [lindex $item 1]
        }
    }
    unset ::replyqueue($fd)
    close $fd
}

proc accept {fd addr port} {
    init_client $fd
    fconfigure $fd -blocking 0 -translation binary -encoding binary
    fileevent $fd readable [list read_request $fd]
}

proc lookup_command name {
    foreach ct $::cmdtable {
        if {$name eq [lindex $ct 0]} {
            return $ct
        }
    }
    return {}
}

proc read_request fd {
    if [eof $fd] {
        close_client $fd
        return
    }

    # Handle first line request, that is *<argc>
    if {$::mbulk($fd) == -1} {
        set req [string trim [gets $fd] "\r\n "]
        if {$req eq {}} return
        set ::mbulk($fd) [string range $req 1 end]
        return
    }

    # Read argument length
    if {$::readlen($fd) == -1} {
        set req [string trim [gets $fd] "\r\n "]
        set ::readlen($fd) [expr [string range $req 1 end]+2]
        lappend ::argvector($fd) {}
    }

    if {[llength $::argvector($fd)] > 1} {
        set cmd [lookup_command [lindex $::argvector($fd) 0]]
    } else {
        set cmd {}
    }

    if {[llength $cmd]
        && [lindex $cmd 2] eq {bulk}
        && [lindex $cmd 1] == [llength $::argvector($fd)]} {
        # Create the temp file that will hold the argument data
        if {$::bulkfd($fd) eq {}} {
            set ::bulkfile($fd) [get_tmp_file]
            set ::bulkfd($fd) [open $::bulkfile($fd) w]
            fconfigure $::bulkfd($fd) -translation binary -encoding binary
        }
        # Read current argument on file
        while {$::readlen($fd)} {
            set readlen $::readlen($fd)
            if {$readlen > 4096} {set readlen 4096}
            set buf [read $fd $readlen]
            if {$buf eq {}} break
            incr ::readlen($fd) -[string length $buf]
            if {$::readlen($fd) < 2} {
                set buf [string range $buf 0 end-[expr {2-$::readlen($fd)}]]
            }
            puts -nonewline $::bulkfd($fd) $buf
        }
        if {$::readlen($fd) == 0} {
            lset ::argvector($fd) end $::bulkfile($fd)
            set ::readlen($fd) -1
        }
    } else {
        # Read current argument in memory
        set buf [read $fd $::readlen($fd)]
        incr ::readlen($fd) -[string length $buf]
        if {$::readlen($fd) < 2} {
            set buf [string range $buf 0 end-[expr {2-$::readlen($fd)}]]
        }
        set arg [lindex $::argvector($fd) end]
        append arg $buf
        lset ::argvector($fd) end $arg
        if {$::readlen($fd) == 0} {
            set ::readlen($fd) -1
        }
    }

    # If our argument vector is complete, exec the command.
    if {$::readlen($fd) == -1 && $::mbulk($fd) == [llength $::argvector($fd)]} {
        set cmd [lookup_command [lindex $::argvector($fd) 0]]
        if {[llength $cmd] == 0} {
            add_reply $fd "-ERR invalid command name"
        } elseif {[llength $::argvector($fd)] != [lindex $cmd 1]} {
            add_reply $fd "-ERR invalid number of arguments for command"
        } else {
            cmd_[lindex $cmd 0] $fd $::argvector($fd)
        }
        reset_client $fd
    }
}

proc write_reply fd {
    while 1 {
        if {[llength $::replyqueue($fd)] == 0} {
            fileevent $fd writable {}
            return;
        }
        set q [lrange $::replyqueue($fd) 1 end]
        set item [lindex $::replyqueue($fd) 0]
        set type [lindex $item 0]
        set value [lindex $item 1]

        if {$type eq {buf}} {
            puts -nonewline $fd $value
            flush $fd
            set ::replyqueue($fd) $q ;# Consume the item
            continue
        } else {
            set buf [read $value 16384]
            if {$buf eq {}} {
                set ::replyqueue($fd) $q ;# Consume the item
                close $value
            } else {
                puts -nonewline $fd $buf
                flush $fd
                break
            }
        }
    }
}

proc setup_bulk_read {fd argv len} {
    set ::argvector($fd) $argv
    set ::readlen($fd) [expr {$len+2}]  ;# Add two bytes for CRLF
}

proc wakeup_writable_handler fd {
    if {[llength $::replyqueue($fd)] == 0} {
        fileevent $fd writable [list write_reply $fd]
    }
}

proc add_reply_raw {fd msg} {
    wakeup_writable_handler $fd
    lappend ::replyqueue($fd) [list buf $msg]
}

proc add_reply {fd msg} {
    add_reply_raw $fd "$msg\r\n"
}

proc add_reply_int {fd int} {
    add_reply $fd ":$int"
}

proc add_reply_file {fd filename} {
    wakeup_writable_handler $fd
    set keyfd [open $filename]
    fconfigure $keyfd -translation binary -encoding binary
    seek $keyfd 0 end
    set len [tell $keyfd]
    seek $keyfd 0
    lappend ::replyqueue($fd) [list buf "\$$len\r\n"]
    lappend ::replyqueue($fd) [list file $keyfd]
    lappend ::replyqueue($fd) [list buf "\r\n"]
}

set ::cmdtable {
    {ping 1 inline}
    {quit 1 inline}
    {set 3 bulk}
    {get 2 inline}
    {exists 2 inline}
    {del 2 inline}
    {incrby 3 inline}
}

################################################################################
# DB API
################################################################################

proc get_tmp_file {} {
    file join $::root tmp temp_[incr ::tmpfileid]
}

proc file_for_key {key} {
    set sha1 [sha1_hex $key]
    file join $::root [string range $sha1 0 1] [string range $sha1 2 3] $sha1
}

proc create_dir_for_key {key} {
    set sha1 [sha1_hex $key]
    set dir1 [file join $::root [string range $sha1 0 1]]
    set dir2 [file join $dir1 [string range $sha1 2 3]]
    catch {file mkdir $dir1}
    catch {file mkdir $dir2}
}

proc create_key_from_file {fd key filename} {
    # Try to move the file in an optimistic way like if the two directories
    # already exist. Otherwise create it and retry... in the long run there is
    # an high probability that the directory exists as there are just 65563
    # dirs in the database.
    if {[catch {
        file rename -force -- $filename [file_for_key $key]
    }]} {
        create_dir_for_key $key
        file rename -force -- $filename [file_for_key $key]
    }
}

proc key_exists {key} {
    file exists [file_for_key $key]
}

proc key_del {key} {
    file delete [file_for_key $key]
}

proc key_get_content {key} {
    set fd [open [file_for_key $key]]
    fconfigure $fd -translation binary -encoding binary
    set buf [read $fd]
    close $fd
    return $buf
}

proc key_set_content {key buf} {
    set tmpfile [get_tmp_file]
    if {[catch {
        set fd [open $tmpfile w]
    }]} {
        create_dir_for_key $key
        set fd [open $tmpfile w]
    }
    fconfigure $fd -translation binary -encoding binary
    puts -nonewline $fd $buf
    close $fd
    file rename -force -- $tmpfile [file_for_key $key]
}

################################################################################
# Commands implementation
################################################################################

proc cmd_ping {fd argv} {
    add_reply $fd "+PONG"
}

proc cmd_quit {fd argv} {
    add_reply $fd "+BYE"
    close_client $fd
}

proc cmd_exists {fd argv} {
    add_reply_int $fd [key_exists [lindex $argv 1]]
}

proc cmd_set {fd argv} {
    create_key_from_file $fd [lindex $argv 1] [lindex $argv 2]
    add_reply $fd "+OK"
}

proc cmd_get {fd argv} {
    set filename [file_for_key [lindex $argv 1]]
    if {![file exists $filename]} {
        add_reply $fd "$-1"
    } else {
        add_reply_file $fd $filename
    }
}

proc cmd_del {fd argv} {
    if {![key_exists [lindex $argv 1]]} {
        add_reply_int $fd 0
    } else {
        key_del [lindex $argv 1]
        add_reply_int $fd 1
    }
}

proc cmd_incrby {fd argv} {
    set key [lindex $argv 1]
    set increment [lindex $argv 2]

    if {![string is integer $increment]} {
        add_reply $fd "-ERR increment argument should be an integer"
        return
    }
    if {[key_exists $key]} {
        set val [key_get_content $key]
    } else {
        set val 0
    }
    if {![string is integer $val]} {
        add_reply $fd "-ERR Can't increment the value at $key: not an integer"
        return
    }
    incr val $increment
    key_set_content $key $val
    add_reply_int $fd $val
}

################################################################################
# Initialization
################################################################################

proc initialize {} {
    file mkdir [file join $::root tmp]
    set ::listensocket [socket -server accept 6379]
}

proc cron {} {
    # Todo timeout clients timeout
    log "bigdis: [array size ::clients] connected clients"
    after 1000 cron
}

proc main {} {
    log "Server started"
    initialize
    cron
}

main
vwait forever

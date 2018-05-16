#!/bin/sh
export LD_LIBRARY_PATH=".:$LD_LIBRARY_PATH./libadapter:./liberl_vcall:./libfcm"
erl +K true +P 655350 +hms 4194304 +hmbs 4194304 -detached -boot start_sasl -config sasl -setcookie abc -name webserver@192.168.31.241 -env ERL_LIBS ../lib

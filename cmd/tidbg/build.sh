#!/bin/bash

path=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

set -x
go build -o ~/bin/tidbg "$path"

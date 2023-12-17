cd src/raft

export GOPATH=/Users/tzq0301/Code/Go/NJU-DisSys-2017
export GOPROXY=https://goproxy.cn
export GO111MODULE=off

go test -run Election

cd - >> /dev/null
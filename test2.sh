cd src/raft

export GOPATH=/Users/tzq0301/Code/Go/NJU-DisSys-2017
export GOPROXY=https://goproxy.cn
export GO111MODULE=off

echo '========================================='
echo '                test2                    '
echo '========================================='
go test -run FailNoAgree
echo '-----------------------------------------'
go test -run ConcurrentStarts
echo '-----------------------------------------'
go test -run Rejoin
echo '-----------------------------------------'
go test -run Backup

cd - >> /dev/null
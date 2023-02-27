.PHONY: build run-server tpc-local-test tpc ycsb local tmp msgtest exp mi heu skew

all: test build

# docker exec -it fc-node make build
build:
	@go build -o ./bin/fc-server ./fc-server/main.go

# docker run -dt --name=fc-node --network=host --cap-add=NET_ADMIN --cap-add=NET_RAW fc sh
# docker exec -it fc-node tc qdisc add dev ens1f1 root handle 1: prio bands 4
# docker exec -it fc-node tc qdisc add dev ens1f1 parent 1:4 handle 40: netem delay 10ms
# docker exec -it fc-node tc filter add dev ens1f1 protocol ip parent 1:0 prio 4 u32 match ip dport 2001 0xffff flowid 1:4
# docker exec -it fc-node tc filter del dev ens1f1 pref 4
# docker exec -it fc-node tc qdisc del dev ens1f1 root


# docker exec -it fc-node tc qdisc add dev eth0 root netem delay 10ms

# docker run --name pg -e POSTGRES_PASSWORD=flexicommit -p 5432:5432 -d postgres

build-docker:
	@docker rmi -f fc
	@rm -f fc-docker.tar
	@docker build -t fc .
	@docker save -o fc-docker.tar fc

delay:
	@tc qdisc add dev lo root handle 1: prio bands 4
	@tc qdisc add dev lo parent 1:4 handle 40: netem delay 10ms 2ms
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6001 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6002 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6003 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6004 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6005 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6006 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6007 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 6008 0xffff flowid 1:4
	@tc filter add dev lo protocol ip parent 1:0 prio 4 u32  match ip dport 5001 0xffff flowid 1:4

rm_delay:
	@tc filter del dev lo pref 4
	@tc qdisc  del dev lo root

test:
	@go test ./storage
	@echo "Storage test pass!"
	@go test ./network/participant
	@echo "The participant side transaction manager test pass!"
	@go test ./network/coordinator
	@echo "The coordinator side transaction manager test pass!"

buildrpc:
	@cd downserver
	@python -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I. rpc.proto
	@protoc --go_out=plugins=grpc:. rpc.proto

exp:
	@make build
	@python3 benchmark/experiment.py

exp-no-build:
	@python3 benchmark/experiment.py

rl:
	@python3 network/detector/main.py 68

profiling:
	@make build
	@./bin/fc-server -node=c -addr=127.0.0.1:5001 -local -p=fc -cpu_prof="fc.prof" -store=mongo

check:
	@make build

package coordinator

import (
	"FC/configs"
	"FC/network/participant"
)

func TestKit() (*Context, []*participant.Context) {
	configs.StoredProcedure = true
	stmt := &Context{}
	paStmt := participant.TestKit()
	var Arg = []string{"*", "*", "127.0.0.1:5001", "5"}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt, paStmt
}

func TPCCTestKit() (*Context, []*participant.Context) {
	stmt := &Context{}
	paStmt := participant.TPCCParticipantKit()
	var Arg = []string{"*", "*", "127.0.0.1:5001"}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt, paStmt
}

func YCSBTestKit() (*Context, []*participant.Context) {
	configs.StoredProcedure = true
	stmt := &Context{}
	paStmt := participant.YCSBParticipantKit()
	var Arg = []string{"*", "*", "127.0.0.1:5001"}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt, paStmt
}

func NormalKit(id string) *Context {
	configs.StoredProcedure = true
	stmt := &Context{}
	var Arg = []string{"*", "*", id}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt
}

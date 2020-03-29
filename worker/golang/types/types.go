package types

type FuncInvoker interface {
	Invoke(funcName string, input []byte) (/* output */ []byte, error)
}

type FuncHandler interface {
	Init(funcInvoker FuncInvoker) error
	Call(input []byte) (/* output */ []byte, error)
}

package main

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type OpCode byte

const (
	OP_NOOP OpCode = iota
	OP_PUSH_INT
	OP_PUSH_STR
	OP_PUSH_BOOL
	OP_PUSH_NULL
	OP_LOAD
	OP_STORE
	OP_PRINT
	OP_INPUT
	OP_WAIT
	OP_CHECK
	OP_JMP
	OP_JMP_IF_FALSE
	OP_CALL
	OP_RET
	OP_POP
	OP_ADD
	OP_SUB
	OP_MUL
	OP_DIV
	OP_EQ
	OP_NEQ
	OP_LT
	OP_GT
	OP_AND
	OP_OR
	OP_NOT
	OP_NEG
	OP_MAKE_LIST
	OP_LOAD_LOCAL
	OP_STORE_LOCAL
	OP_INDEX_GET
	OP_INDEX_SET
	OP_SLICE
)

type Instr struct {
	Op   OpCode
	Str  string
	Int  int
	Bool bool
	Line int
}

type Function struct {
	Params     []string
	ParamTypes []ValueType
	ReturnType ValueType
	LocalCount int
	Code       []Instr
}

type Program struct {
	Version   int
	Main      []Instr
	Functions map[string]Function
}

type tokenKind int

const (
	tokLine tokenKind = iota
	tokLBrace
	tokRBrace
)

type Token struct {
	Kind tokenKind
	Text string
	Line int
}

type Compiler struct {
	tokens []Token
	idx    int

	functions map[string]Function
}

type loopCtx struct {
	start         int
	breakJumps    []int
	continueJumps []int
}

type localScope struct {
	vars   map[string]int
	parent *localScope
}

func newLocalScope(parent *localScope) *localScope {
	return &localScope{vars: map[string]int{}, parent: parent}
}

func (s *localScope) resolve(name string) (int, bool) {
	for cur := s; cur != nil; cur = cur.parent {
		if slot, ok := cur.vars[name]; ok {
			return slot, true
		}
	}
	return 0, false
}

func (s *localScope) getLocal(name string) (int, bool) {
	slot, ok := s.vars[name]
	return slot, ok
}

func (s *localScope) setLocal(name string, slot int) {
	s.vars[name] = slot
}

type compileCtx struct {
	inFunction bool
	scope      *localScope
	nextSlot   int
}

func (ctx *compileCtx) childScope() *compileCtx {
	if ctx == nil {
		return nil
	}
	return &compileCtx{
		inFunction: ctx.inFunction,
		scope:      newLocalScope(ctx.scope),
		nextSlot:   ctx.nextSlot,
	}
}

func (ctx *compileCtx) defineLocal(name string) int {
	if ctx == nil || ctx.scope == nil {
		return -1
	}
	if slot, ok := ctx.scope.getLocal(name); ok {
		return slot
	}
	slot := ctx.nextSlot
	ctx.nextSlot++
	ctx.scope.setLocal(name, slot)
	return slot
}

type ValueType int

const (
	typeUnknown ValueType = iota
	typeInt
	typeString
	typeBool
	typeNull
	typeList
	typeDict
)

func (t ValueType) String() string {
	switch t {
	case typeInt:
		return "int"
	case typeString:
		return "string"
	case typeBool:
		return "bool"
	case typeNull:
		return "null"
	case typeList:
		return "list"
	case typeDict:
		return "dict"
	default:
		return "unknown"
	}
}

type typeEnv struct {
	vars   map[string]ValueType
	parent *typeEnv
}

func newTypeEnv(parent *typeEnv) *typeEnv {
	return &typeEnv{vars: map[string]ValueType{}, parent: parent}
}

func (e *typeEnv) get(name string) (ValueType, bool) {
	for cur := e; cur != nil; cur = cur.parent {
		if v, ok := cur.vars[name]; ok {
			return v, true
		}
	}
	return typeUnknown, false
}

func (e *typeEnv) getLocal(name string) (ValueType, bool) {
	v, ok := e.vars[name]
	return v, ok
}

func (e *typeEnv) setLocal(name string, t ValueType) {
	e.vars[name] = t
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage:")
		fmt.Println("  emerald build <file.emer>")
		fmt.Println("  emerald run <file.emer|file.emec>")
		os.Exit(1)
	}

	cmd := os.Args[1]
	path := os.Args[2]

	switch cmd {
	case "build":
		if !strings.HasSuffix(strings.ToLower(path), ".emer") {
			fmt.Println("build expects a .emer source file")
			os.Exit(1)
		}
		prog, err := compileFile(path)
		if err != nil {
			fmt.Printf("Compile error: %v\n", err)
			os.Exit(1)
		}
		outPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".emec"
		if err := writeProgram(outPath, prog); err != nil {
			fmt.Printf("Write error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Wrote %s\n", outPath)
	case "run":
		if strings.HasSuffix(strings.ToLower(path), ".emec") {
			prog, err := readProgram(path)
			if err != nil {
				fmt.Printf("Load error: %v\n", err)
				os.Exit(1)
			}
			vm := NewVM()
			if _, err := vm.Exec(prog, prog.Main); err != nil {
				fmt.Printf("Runtime error: %v\n", err)
				os.Exit(1)
			}
			return
		}
		if !strings.HasSuffix(strings.ToLower(path), ".emer") {
			fmt.Println("run expects a .emer or .emec file")
			os.Exit(1)
		}
		prog, err := compileFile(path)
		if err != nil {
			fmt.Printf("Compile error: %v\n", err)
			os.Exit(1)
		}
		vm := NewVM()
		if _, err := vm.Exec(prog, prog.Main); err != nil {
			fmt.Printf("Runtime error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Println("Unknown command:", cmd)
		os.Exit(1)
	}
}

func compileFile(path string) (*Program, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	tokens, err := tokenize(string(data))
	if err != nil {
		return nil, err
	}
	c := &Compiler{tokens: tokens, functions: map[string]Function{}}
	if err := c.predeclareFunctions(); err != nil {
		return nil, err
	}
	mainCode := []Instr{}
	globalTypes := newTypeEnv(nil)
	if err := c.compileBlock(&mainCode, nil, globalTypes, typeUnknown, nil); err != nil {
		return nil, err
	}
	prog := &Program{Version: 2, Main: mainCode, Functions: c.functions}
	return prog, nil
}

func writeProgram(path string, prog *Program) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	return enc.Encode(prog)
}

func readProgram(path string) (*Program, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var prog Program
	if err := dec.Decode(&prog); err != nil {
		return nil, err
	}
	return &prog, nil
}

func tokenize(src string) ([]Token, error) {
	tokens := []Token{}
	var line strings.Builder
	inString := false
	var quote rune
	lineNum := 1

	flushLine := func() {
		s := strings.TrimSpace(line.String())
		if s != "" {
			tokens = append(tokens, Token{Kind: tokLine, Text: s, Line: lineNum})
		}
		line.Reset()
	}

	for _, r := range src {
		if inString {
			line.WriteRune(r)
			if r == quote {
				inString = false
			}
			continue
		}
		switch r {
		case '\'', '"':
			inString = true
			quote = r
			line.WriteRune(r)
		case '{':
			flushLine()
			tokens = append(tokens, Token{Kind: tokLBrace, Text: "{", Line: lineNum})
		case '}':
			flushLine()
			tokens = append(tokens, Token{Kind: tokRBrace, Text: "}", Line: lineNum})
		case '\n':
			flushLine()
			lineNum++
		case '\r':
		default:
			line.WriteRune(r)
		}
	}
	flushLine()
	return tokens, nil
}

func (c *Compiler) next() *Token {
	if c.idx >= len(c.tokens) {
		return nil
	}
	t := &c.tokens[c.idx]
	c.idx++
	return t
}

func (c *Compiler) peek() *Token {
	if c.idx >= len(c.tokens) {
		return nil
	}
	return &c.tokens[c.idx]
}

func (c *Compiler) compileBlock(code *[]Instr, loopStack []loopCtx, types *typeEnv, retType ValueType, ctx *compileCtx) error {
	for {
		t := c.next()
		if t == nil {
			return nil
		}
		if t.Kind == tokRBrace {
			return nil
		}
		if t.Kind == tokLBrace {
			return fmt.Errorf("line %d: unexpected '{'", t.Line)
		}

		line := t.Text
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "--") {
			continue
		}

		if strings.HasPrefix(line, "fnc ") {
			name, params, paramTypes, fnRet, err := parseFnDecl(line)
			if err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			if p := c.next(); p == nil || p.Kind != tokLBrace {
				return fmt.Errorf("line %d: fnc must be followed by '{'", t.Line)
			}
			fnCode := []Instr{}
			fnTypes := newTypeEnv(types)
			fnCtx := &compileCtx{inFunction: true, scope: newLocalScope(nil)}
			for i, p := range params {
				slot := fnCtx.defineLocal(p)
				if slot != i {
					return fmt.Errorf("line %d: internal error: parameter slot mismatch", t.Line)
				}
				if i < len(paramTypes) {
					fnTypes.setLocal(p, paramTypes[i])
				} else {
					fnTypes.setLocal(p, typeUnknown)
				}
			}
			if err := c.compileBlock(&fnCode, nil, fnTypes, fnRet, fnCtx); err != nil {
				return err
			}
			c.functions[name] = Function{Params: params, ParamTypes: paramTypes, ReturnType: fnRet, LocalCount: fnCtx.nextSlot, Code: fnCode}
			continue
		}

		if strings.HasPrefix(line, "var") {
			if err := c.compileVar(line, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			continue
		}

		if strings.HasPrefix(line, "print(") && strings.HasSuffix(line, ")") {
			start := len(*code)
			inner := strings.TrimSpace(line[len("print(") : len(line)-1])
			if err := c.compileExpr(inner, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			*code = append(*code, Instr{Op: OP_PRINT, Line: t.Line})
			setInstrLine(*code, start, t.Line)
			continue
		}

		if strings.HasPrefix(line, "input(plc(") && strings.HasSuffix(line, "))") {
			start := len(*code)
			inner := strings.TrimSpace(line[len("input(plc(") : len(line)-2])
			s, ok := parseStringLiteral(inner)
			if !ok {
				return fmt.Errorf("line %d: input requires a string literal prompt", t.Line)
			}
			*code = append(*code, Instr{Op: OP_PUSH_STR, Str: s, Line: t.Line})
			*code = append(*code, Instr{Op: OP_CALL, Str: "input", Int: 1, Line: t.Line})
			*code = append(*code, Instr{Op: OP_POP, Line: t.Line})
			setInstrLine(*code, start, t.Line)
			continue
		}

		if strings.HasPrefix(line, "wait(") && strings.HasSuffix(line, ")") {
			start := len(*code)
			inner := strings.TrimSpace(line[len("wait(") : len(line)-1])
			exprNode, err := parseExprAst(inner)
			if err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			if types != nil {
				et, err := c.inferExprType(exprNode, types)
				if err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if et != typeUnknown && et != typeInt {
					return fmt.Errorf("line %d: wait expects int milliseconds", t.Line)
				}
			}
			if err := c.compileExprNode(exprNode, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			*code = append(*code, Instr{Op: OP_WAIT, Line: t.Line})
			setInstrLine(*code, start, t.Line)
			continue
		}

		if strings.HasPrefix(line, "check ") {
			start := len(*code)
			inner := strings.TrimSpace(strings.TrimPrefix(line, "check "))
			if err := c.compileExpr(inner, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			*code = append(*code, Instr{Op: OP_CHECK, Str: inner, Line: t.Line})
			setInstrLine(*code, start, t.Line)
			continue
		}

		if line == "brk" {
			if len(loopStack) == 0 {
				return fmt.Errorf("line %d: brk used outside loop", t.Line)
			}
			idx := len(*code)
			*code = append(*code, Instr{Op: OP_JMP, Int: -1, Line: t.Line})
			loopStack[len(loopStack)-1].breakJumps = append(loopStack[len(loopStack)-1].breakJumps, idx)
			continue
		}

		if line == "cont" {
			if len(loopStack) == 0 {
				return fmt.Errorf("line %d: cont used outside loop", t.Line)
			}
			idx := len(*code)
			*code = append(*code, Instr{Op: OP_JMP, Int: -1, Line: t.Line})
			loopStack[len(loopStack)-1].continueJumps = append(loopStack[len(loopStack)-1].continueJumps, idx)
			continue
		}

		if line == "return" || strings.HasPrefix(line, "return ") {
			start := len(*code)
			expr := strings.TrimSpace(strings.TrimPrefix(line, "return"))
			if expr != "" {
				exprNode, err := parseExprAst(expr)
				if err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if types != nil {
					exprType, err := c.inferExprType(exprNode, types)
					if err != nil {
						return fmt.Errorf("line %d: %w", t.Line, err)
					}
					if retType != typeUnknown && exprType == typeUnknown {
						return fmt.Errorf("line %d: type mismatch: return is %s, got %s", t.Line, retType.String(), exprType.String())
					}
					if retType != typeUnknown && !assignable(retType, exprType) {
						return fmt.Errorf("line %d: type mismatch: return is %s, got %s", t.Line, retType.String(), exprType.String())
					}
				}
				if err := c.compileExprNode(exprNode, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_RET, Line: t.Line})
			} else {
				if retType != typeUnknown && !assignable(retType, typeNull) {
					return fmt.Errorf("line %d: type mismatch: return is %s, got %s", t.Line, retType.String(), typeNull.String())
				}
				*code = append(*code, Instr{Op: OP_PUSH_NULL, Line: t.Line})
				*code = append(*code, Instr{Op: OP_RET, Line: t.Line})
			}
			setInstrLine(*code, start, t.Line)
			continue
		}

		if strings.HasPrefix(line, "if(") && strings.HasSuffix(line, ")") {
			cond := strings.TrimSpace(line[len("if(") : len(line)-1])
			start := len(*code)
			if err := c.compileExpr(cond, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			jmpIfFalseIdx := len(*code)
			*code = append(*code, Instr{Op: OP_JMP_IF_FALSE, Int: -1, Line: t.Line})
			if p := c.next(); p == nil || p.Kind != tokLBrace {
				return fmt.Errorf("line %d: if must be followed by '{'", t.Line)
			}
			ifCtx := ctx.childScope()
			if err := c.compileBlock(code, loopStack, newTypeEnv(types), retType, ifCtx); err != nil {
				return err
			}
			if ifCtx != nil && ctx != nil && ifCtx.nextSlot > ctx.nextSlot {
				ctx.nextSlot = ifCtx.nextSlot
			}
			endJumps := []int{len(*code)}
			*code = append(*code, Instr{Op: OP_JMP, Int: -1, Line: t.Line})
			setInstrLine(*code, start, t.Line)

			for {
				n := c.peek()
				if n == nil || n.Kind != tokLine {
					break
				}
				nextLine := strings.TrimSpace(n.Text)
				if strings.HasPrefix(nextLine, "else if(") && strings.HasSuffix(nextLine, ")") {
					c.next()
					(*code)[jmpIfFalseIdx].Int = len(*code)
					cond2 := strings.TrimSpace(nextLine[len("else if(") : len(nextLine)-1])
					start2 := len(*code)
					if err := c.compileExpr(cond2, code, types, ctx, n.Line); err != nil {
						return fmt.Errorf("line %d: %w", n.Line, err)
					}
					jmpIfFalseIdx = len(*code)
					*code = append(*code, Instr{Op: OP_JMP_IF_FALSE, Int: -1, Line: n.Line})
					if p := c.next(); p == nil || p.Kind != tokLBrace {
						return fmt.Errorf("line %d: else if must be followed by '{'", n.Line)
					}
					elseIfCtx := ctx.childScope()
					if err := c.compileBlock(code, loopStack, newTypeEnv(types), retType, elseIfCtx); err != nil {
						return err
					}
					if elseIfCtx != nil && ctx != nil && elseIfCtx.nextSlot > ctx.nextSlot {
						ctx.nextSlot = elseIfCtx.nextSlot
					}
					endJumps = append(endJumps, len(*code))
					*code = append(*code, Instr{Op: OP_JMP, Int: -1, Line: n.Line})
					setInstrLine(*code, start2, n.Line)
					continue
				}
				if nextLine == "else" {
					c.next()
					(*code)[jmpIfFalseIdx].Int = len(*code)
					if p := c.next(); p == nil || p.Kind != tokLBrace {
						return fmt.Errorf("line %d: else must be followed by '{'", n.Line)
					}
					elseCtx := ctx.childScope()
					if err := c.compileBlock(code, loopStack, newTypeEnv(types), retType, elseCtx); err != nil {
						return err
					}
					if elseCtx != nil && ctx != nil && elseCtx.nextSlot > ctx.nextSlot {
						ctx.nextSlot = elseCtx.nextSlot
					}
					jmpIfFalseIdx = -1
					break
				}
				break
			}
			if jmpIfFalseIdx != -1 {
				(*code)[jmpIfFalseIdx].Int = len(*code)
			}
			end := len(*code)
			for _, idx := range endJumps {
				(*code)[idx].Int = end
			}
			continue
		}

		if strings.HasPrefix(line, "for(") && strings.HasSuffix(line, ")") {
			header := strings.TrimSpace(line[len("for(") : len(line)-1])
			if itName, itExpr, ok := parseForInHeader(header); ok {
				if p := c.next(); p == nil || p.Kind != tokLBrace {
					return fmt.Errorf("line %d: for must be followed by '{'", t.Line)
				}
				start := len(*code)
				iterList := fmt.Sprintf("__iter_list_%d", start)
				iterIdx := fmt.Sprintf("__iter_idx_%d", start)
				if err := c.compileExpr(itExpr, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.emitStore(iterList, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_PUSH_INT, Int: 0, Line: t.Line})
				if err := c.emitStore(iterIdx, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				loopStart := len(*code)
				if err := c.emitLoad(iterIdx, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.emitLoad(iterList, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_CALL, Str: "len", Int: 1, Line: t.Line})
				*code = append(*code, Instr{Op: OP_LT, Line: t.Line})
				jmpIfFalseIdx := len(*code)
				*code = append(*code, Instr{Op: OP_JMP_IF_FALSE, Int: -1, Line: t.Line})
				if err := c.emitLoad(iterList, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.emitLoad(iterIdx, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_INDEX_GET, Line: t.Line})
				bodyCtx := ctx.childScope()
				if err := c.emitStore(itName, code, bodyCtx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				bodyTypes := newTypeEnv(types)
				bodyTypes.setLocal(itName, typeUnknown)
				ctxLoop := loopCtx{start: -1}
				loopStack = append(loopStack, ctxLoop)
				if err := c.compileBlock(code, loopStack, bodyTypes, retType, bodyCtx); err != nil {
					return err
				}
				stepStart := len(*code)
				if err := c.emitLoad(iterIdx, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_PUSH_INT, Int: 1, Line: t.Line})
				*code = append(*code, Instr{Op: OP_ADD, Line: t.Line})
				if err := c.emitStore(iterIdx, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_JMP, Int: loopStart, Line: t.Line})
				loopEnd := len(*code)
				(*code)[jmpIfFalseIdx].Int = loopEnd
				cur := loopStack[len(loopStack)-1]
				for _, bi := range cur.breakJumps {
					(*code)[bi].Int = loopEnd
				}
				for _, ci := range cur.continueJumps {
					(*code)[ci].Int = stepStart
				}
				loopStack = loopStack[:len(loopStack)-1]
				if bodyCtx != nil && ctx != nil && bodyCtx.nextSlot > ctx.nextSlot {
					ctx.nextSlot = bodyCtx.nextSlot
				}
				setInstrLine(*code, start, t.Line)
				continue
			}

			cond := header
			if p := c.next(); p == nil || p.Kind != tokLBrace {
				return fmt.Errorf("line %d: for must be followed by '{'", t.Line)
			}
			loopStart := len(*code)
			hasCond := strings.ToLower(cond) != "true"
			var jmpIfFalseIdx int
			start := len(*code)
			if hasCond {
				if err := c.compileExpr(cond, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				jmpIfFalseIdx = len(*code)
				*code = append(*code, Instr{Op: OP_JMP_IF_FALSE, Int: -1, Line: t.Line})
			}

			loop := loopCtx{start: loopStart}
			loopStack = append(loopStack, loop)
			loopCtxState := ctx.childScope()
			if err := c.compileBlock(code, loopStack, newTypeEnv(types), retType, loopCtxState); err != nil {
				return err
			}
			*code = append(*code, Instr{Op: OP_JMP, Int: loopStart, Line: t.Line})
			loopEnd := len(*code)
			if hasCond {
				(*code)[jmpIfFalseIdx].Int = loopEnd
			}
			cur := loopStack[len(loopStack)-1]
			for _, bi := range cur.breakJumps {
				(*code)[bi].Int = loopEnd
			}
			for _, ci := range cur.continueJumps {
				(*code)[ci].Int = loopStart
			}
			loopStack = loopStack[:len(loopStack)-1]
			if loopCtxState != nil && ctx != nil && loopCtxState.nextSlot > ctx.nextSlot {
				ctx.nextSlot = loopCtxState.nextSlot
			}
			setInstrLine(*code, start, t.Line)
			continue
		}

		if strings.HasPrefix(line, "while(") && strings.HasSuffix(line, ")") {
			cond := strings.TrimSpace(line[len("while(") : len(line)-1])
			if p := c.next(); p == nil || p.Kind != tokLBrace {
				return fmt.Errorf("line %d: while must be followed by '{'", t.Line)
			}
			loopStart := len(*code)
			start := len(*code)
			if err := c.compileExpr(cond, code, types, ctx, t.Line); err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			jmpIfFalseIdx := len(*code)
			*code = append(*code, Instr{Op: OP_JMP_IF_FALSE, Int: -1, Line: t.Line})
			loop := loopCtx{start: loopStart}
			loopStack = append(loopStack, loop)
			loopCtxState := ctx.childScope()
			if err := c.compileBlock(code, loopStack, newTypeEnv(types), retType, loopCtxState); err != nil {
				return err
			}
			*code = append(*code, Instr{Op: OP_JMP, Int: loopStart, Line: t.Line})
			loopEnd := len(*code)
			(*code)[jmpIfFalseIdx].Int = loopEnd
			cur := loopStack[len(loopStack)-1]
			for _, bi := range cur.breakJumps {
				(*code)[bi].Int = loopEnd
			}
			for _, ci := range cur.continueJumps {
				(*code)[ci].Int = loopStart
			}
			loopStack = loopStack[:len(loopStack)-1]
			if loopCtxState != nil && ctx != nil && loopCtxState.nextSlot > ctx.nextSlot {
				ctx.nextSlot = loopCtxState.nextSlot
			}
			setInstrLine(*code, start, t.Line)
			continue
		}

		if target, expr, ok := splitAssignment(line); ok {
			if target == "" {
				return fmt.Errorf("line %d: invalid assignment", t.Line)
			}
			if strings.Contains(target, "(") || strings.Contains(target, ")") {
				return fmt.Errorf("line %d: type annotations are only allowed in var declarations", t.Line)
			}
			targetNode, err := parseExprAst(target)
			if err != nil {
				return fmt.Errorf("line %d: invalid assignment target", t.Line)
			}
			exprNode, err := parseExprAst(expr)
			if err != nil {
				return fmt.Errorf("line %d: %w", t.Line, err)
			}
			if types != nil {
				exprType, err := c.inferExprType(exprNode, types)
				if err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				switch targetNode.Kind {
				case exprIdent:
					name := targetNode.Name
					if existing, ok := types.getLocal(name); ok {
						if existing != typeUnknown && exprType == typeUnknown {
							return fmt.Errorf("line %d: type mismatch: %s is %s, got %s", t.Line, name, existing.String(), exprType.String())
						}
						if !assignable(existing, exprType) {
							return fmt.Errorf("line %d: type mismatch: %s is %s, got %s", t.Line, name, existing.String(), exprType.String())
						}
					} else {
						if exprType != typeUnknown {
							types.setLocal(name, exprType)
						} else {
							types.setLocal(name, typeUnknown)
						}
					}
				case exprIndex:
					targetType, err := c.inferExprType(targetNode.Left, types)
					if err != nil {
						return fmt.Errorf("line %d: %w", t.Line, err)
					}
					if targetType != typeUnknown && targetType != typeList && targetType != typeDict {
						return fmt.Errorf("line %d: index assignment expects list or dict target", t.Line)
					}
				default:
					return fmt.Errorf("line %d: invalid assignment target", t.Line)
				}
			}

			start := len(*code)
			switch targetNode.Kind {
			case exprIdent:
				if err := c.compileExprNode(exprNode, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.emitStore(targetNode.Name, code, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
			case exprIndex:
				if err := c.compileExprNode(targetNode.Left, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.compileExprNode(targetNode.Right, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				if err := c.compileExprNode(exprNode, code, types, ctx, t.Line); err != nil {
					return fmt.Errorf("line %d: %w", t.Line, err)
				}
				*code = append(*code, Instr{Op: OP_INDEX_SET, Line: t.Line})
			default:
				return fmt.Errorf("line %d: invalid assignment target", t.Line)
			}
			setInstrLine(*code, start, t.Line)
			continue
		}

		start := len(*code)
		if err := c.compileExpr(line, code, types, ctx, t.Line); err == nil {
			*code = append(*code, Instr{Op: OP_POP, Line: t.Line})
			setInstrLine(*code, start, t.Line)
			continue
		}

		return fmt.Errorf("line %d: unknown statement: %s", t.Line, line)
	}
}

func setInstrLine(code []Instr, start int, line int) {
	for i := start; i < len(code); i++ {
		if code[i].Line == 0 {
			code[i].Line = line
		}
	}
}

func parseForInHeader(header string) (string, string, bool) {
	inString := false
	var quote byte
	parenDepth := 0
	bracketDepth := 0
	for i := 0; i < len(header); i++ {
		ch := header[i]
		if inString {
			if ch == quote {
				inString = false
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inString = true
			quote = ch
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		default:
			if parenDepth == 0 && bracketDepth == 0 && strings.HasPrefix(header[i:], " in ") {
				name := strings.TrimSpace(header[:i])
				expr := strings.TrimSpace(header[i+4:])
				if isIdent(name) && expr != "" {
					return name, expr, true
				}
				return "", "", false
			}
		}
	}
	return "", "", false
}

func splitAssignment(line string) (string, string, bool) {
	inString := false
	var quote byte
	parenDepth := 0
	bracketDepth := 0
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if inString {
			if ch == quote {
				inString = false
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inString = true
			quote = ch
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '=':
			if parenDepth == 0 && bracketDepth == 0 {
				if i+1 < len(line) && line[i+1] == '=' {
					i++
					continue
				}
				if i > 0 && (line[i-1] == '=' || line[i-1] == '!') {
					continue
				}
				left := strings.TrimSpace(line[:i])
				right := strings.TrimSpace(line[i+1:])
				return left, right, true
			}
		}
	}
	return "", "", false
}

func parseFnDecl(line string) (string, []string, []ValueType, ValueType, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(line, "fnc "))
	if rest == "" {
		return "", nil, nil, typeUnknown, errors.New("fnc requires a name")
	}
	name := rest
	params := []string{}
	paramTypes := []ValueType{}
	retType := typeUnknown
	if i := strings.Index(rest, "("); i != -1 {
		closeIdx := findMatchingParen(rest, i)
		if closeIdx == -1 {
			return "", nil, nil, typeUnknown, errors.New("fnc params must end with ')' before '{'")
		}
		name = strings.TrimSpace(rest[:i])
		inner := strings.TrimSpace(rest[i+1 : closeIdx])
		if inner != "" {
			parts := splitTopLevel(inner, ',')
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				pname, ptype, err := parseParamSpec(p)
				if err != nil {
					return "", nil, nil, typeUnknown, err
				}
				params = append(params, pname)
				paramTypes = append(paramTypes, ptype)
			}
		}
		restAfter := strings.TrimSpace(rest[closeIdx+1:])
		if restAfter != "" {
			if !strings.HasPrefix(restAfter, "(") || !strings.HasSuffix(restAfter, ")") {
				return "", nil, nil, typeUnknown, errors.New("fnc return type must be in parentheses")
			}
			rt := strings.TrimSpace(restAfter[1 : len(restAfter)-1])
			if rt == "" {
				return "", nil, nil, typeUnknown, errors.New("fnc return type is empty")
			}
			parsed, ok := parseTypeName(rt)
			if !ok {
				return "", nil, nil, typeUnknown, fmt.Errorf("unknown type: %s", rt)
			}
			retType = parsed
		}
	}
	if name == "" {
		return "", nil, nil, typeUnknown, errors.New("fnc requires a name")
	}
	if !isIdent(name) {
		return "", nil, nil, typeUnknown, errors.New("invalid fnc name")
	}
	if hasDuplicate(params) {
		return "", nil, nil, typeUnknown, errors.New("duplicate parameter name")
	}
	return name, params, paramTypes, retType, nil
}

func (c *Compiler) compileVar(line string, code *[]Instr, types *typeEnv, ctx *compileCtx, lineNo int) error {
	trimmed := strings.TrimSpace(line)
	if trimmed == "var" {
		if p := c.next(); p == nil || p.Kind != tokLBrace {
			return errors.New("var block must be followed by '{'")
		}
		itemsText := c.collectBlockText()
		specs := splitTopLevel(itemsText, ',')
		for _, spec := range specs {
			spec = strings.TrimSpace(spec)
			if spec == "" {
				continue
			}
			if err := c.compileVarSpec(spec, code, types, ctx, lineNo); err != nil {
				return err
			}
		}
		return nil
	}

	if strings.HasPrefix(trimmed, "var ") {
		spec := strings.TrimSpace(strings.TrimPrefix(trimmed, "var "))
		if strings.HasPrefix(spec, "{") {
			itemsText := strings.TrimSpace(strings.TrimPrefix(spec, "{"))
			if strings.HasSuffix(itemsText, "}") {
				itemsText = strings.TrimSpace(strings.TrimSuffix(itemsText, "}"))
			} else {
				itemsText += " " + c.collectBlockText()
			}
			specs := splitTopLevel(itemsText, ',')
			for _, s := range specs {
				s = strings.TrimSpace(s)
				if s == "" {
					continue
				}
				if err := c.compileVarSpec(s, code, types, ctx, lineNo); err != nil {
					return err
				}
			}
			return nil
		}
		return c.compileVarSpec(spec, code, types, ctx, lineNo)
	}

	return nil
}

func (c *Compiler) collectBlockText() string {
	var b strings.Builder
	depth := 1
	for depth > 0 {
		t := c.next()
		if t == nil {
			break
		}
		if t.Kind == tokLBrace {
			depth++
			continue
		}
		if t.Kind == tokRBrace {
			depth--
			if depth == 0 {
				break
			}
			continue
		}
		if t.Kind == tokLine {
			if b.Len() > 0 {
				b.WriteString(" ")
			}
			b.WriteString(t.Text)
		}
	}
	return b.String()
}
func (c *Compiler) compileVarSpec(spec string, code *[]Instr, types *typeEnv, ctx *compileCtx, lineNo int) error {
	name, typ, expr, hasExpr, err := parseVarSpec(spec)
	if err != nil {
		return err
	}
	var declaredType ValueType
	if typ != "" {
		var ok bool
		declaredType, ok = parseTypeName(typ)
		if !ok {
			return fmt.Errorf("unknown type: %s", typ)
		}
	}
	if !hasExpr {
		if types != nil {
			if declaredType == typeUnknown {
				types.setLocal(name, typeUnknown)
			} else {
				types.setLocal(name, declaredType)
			}
		}
		*code = append(*code, Instr{Op: OP_PUSH_NULL, Line: lineNo})
		if ctx != nil && ctx.inFunction && ctx.scope != nil {
			slot, ok := ctx.scope.getLocal(name)
			if !ok {
				slot = ctx.defineLocal(name)
			}
			*code = append(*code, Instr{Op: OP_STORE_LOCAL, Int: slot, Line: lineNo})
		} else {
			*code = append(*code, Instr{Op: OP_STORE, Str: name, Line: lineNo})
		}
		return nil
	}
	exprNode, err := parseExprAst(expr)
	if err != nil {
		return err
	}
	if types != nil {
		exprType, err := c.inferExprType(exprNode, types)
		if err != nil {
			return err
		}
		if declaredType != typeUnknown {
			if exprType == typeUnknown {
				return fmt.Errorf("type mismatch: %s is %s, got %s", name, declaredType.String(), exprType.String())
			}
			if !assignable(declaredType, exprType) {
				return fmt.Errorf("type mismatch: %s is %s, got %s", name, declaredType.String(), exprType.String())
			}
			types.setLocal(name, declaredType)
		} else if exprType != typeUnknown {
			types.setLocal(name, exprType)
		} else {
			types.setLocal(name, typeUnknown)
		}
	}
	if err := c.compileExprNode(exprNode, code, types, ctx, lineNo); err != nil {
		return err
	}
	if ctx != nil && ctx.inFunction && ctx.scope != nil {
		slot, ok := ctx.scope.getLocal(name)
		if !ok {
			slot = ctx.defineLocal(name)
		}
		*code = append(*code, Instr{Op: OP_STORE_LOCAL, Int: slot, Line: lineNo})
	} else {
		*code = append(*code, Instr{Op: OP_STORE, Str: name, Line: lineNo})
	}
	return nil
}

func parseVarSpec(spec string) (name string, typ string, expr string, hasExpr bool, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", "", "", false, errors.New("empty var spec")
	}

	nameEnd := strings.IndexAny(spec, "(=")
	if nameEnd == -1 {
		return strings.TrimSpace(spec), "", "", false, nil
	}
	name = strings.TrimSpace(spec[:nameEnd])
	rest := strings.TrimSpace(spec[nameEnd:])
	if strings.HasPrefix(rest, "(") {
		closeIdx := strings.Index(rest, ")")
		if closeIdx == -1 {
			return "", "", "", false, errors.New("missing ')' in type")
		}
		typ = strings.TrimSpace(rest[1:closeIdx])
		rest = strings.TrimSpace(rest[closeIdx+1:])
	}

	if strings.HasPrefix(rest, "=") {
		rest = strings.TrimSpace(rest[1:])
		if strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")") {
			expr = strings.TrimSpace(rest[1 : len(rest)-1])
		} else {
			expr = strings.TrimSpace(rest)
		}
		hasExpr = expr != ""
	}

	if name == "" {
		return "", "", "", false, errors.New("invalid var name")
	}
	if !isIdent(name) {
		return "", "", "", false, errors.New("invalid var name")
	}
	return name, typ, expr, hasExpr, nil
}

func parseTypeName(s string) (ValueType, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "int":
		return typeInt, true
	case "str", "string":
		return typeString, true
	case "bool":
		return typeBool, true
	case "null":
		return typeNull, true
	case "list":
		return typeList, true
	case "dict", "table":
		return typeDict, true
	default:
		return typeUnknown, false
	}
}

func assignable(target ValueType, expr ValueType) bool {
	if target == typeUnknown {
		return true
	}
	if expr == typeUnknown {
		return false
	}
	if expr == typeNull {
		return true
	}
	return target == expr
}

func parseParamSpec(spec string) (string, ValueType, error) {
	name, typ, _, _, err := parseVarSpec(spec)
	if err != nil {
		return "", typeUnknown, err
	}
	if typ == "" {
		return name, typeUnknown, nil
	}
	t, ok := parseTypeName(typ)
	if !ok {
		return "", typeUnknown, fmt.Errorf("unknown type: %s", typ)
	}
	return name, t, nil
}

func findMatchingParen(s string, openIdx int) int {
	if openIdx < 0 || openIdx >= len(s) || s[openIdx] != '(' {
		return -1
	}
	depth := 0
	for i := openIdx; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func hasDuplicate(items []string) bool {
	seen := map[string]struct{}{}
	for _, it := range items {
		if _, ok := seen[it]; ok {
			return true
		}
		seen[it] = struct{}{}
	}
	return false
}

func splitTopLevel(s string, sep rune) []string {
	parts := []string{}
	var cur strings.Builder
	inString := false
	var quote rune
	parenDepth := 0
	bracketDepth := 0

	for _, r := range s {
		if inString {
			cur.WriteRune(r)
			if r == quote {
				inString = false
			}
			continue
		}
		switch r {
		case '\'', '"':
			inString = true
			quote = r
			cur.WriteRune(r)
		case '(':
			parenDepth++
			cur.WriteRune(r)
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			cur.WriteRune(r)
		case '[':
			bracketDepth++
			cur.WriteRune(r)
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			cur.WriteRune(r)
		default:
			if r == sep && parenDepth == 0 && bracketDepth == 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			} else {
				cur.WriteRune(r)
			}
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

// Expression parsing

type exprTokenKind int

const (
	exTokEOF exprTokenKind = iota
	exTokIdent
	exTokNumber
	exTokString
	exTokLParen
	exTokRParen
	exTokLBracket
	exTokRBracket
	exTokComma
	exTokColon
	exTokOp
)

type exprToken struct {
	kind exprTokenKind
	text string
}

type exprParser struct {
	tokens []exprToken
	pos    int
}

type ExprKind int

const (
	exprLiteral ExprKind = iota
	exprIdent
	exprUnary
	exprBinary
	exprCall
	exprList
	exprIndex
	exprSlice
)

type Expr struct {
	Kind     ExprKind
	LitKind  string
	I        int
	S        string
	B        bool
	Name     string
	Op       string
	Left     *Expr
	Right    *Expr
	Target   *Expr
	Start    *Expr
	End      *Expr
	Args     []*Expr
	Elements []*Expr
}

func (p *exprParser) peek() exprToken {
	if p.pos >= len(p.tokens) {
		return exprToken{kind: exTokEOF}
	}
	return p.tokens[p.pos]
}

func (p *exprParser) next() exprToken {
	t := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return t
}

func (p *exprParser) expect(kind exprTokenKind, text string) error {
	t := p.next()
	if t.kind != kind || (text != "" && t.text != text) {
		return fmt.Errorf("unexpected token: %s", t.text)
	}
	return nil
}

func (p *exprParser) parseExpr() (*Expr, error) {
	return p.parseOr()
}

func (p *exprParser) parseOr() (*Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && t.text == "or" {
			p.next()
			right, err := p.parseAnd()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: "or", Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseAnd() (*Expr, error) {
	left, err := p.parseEquality()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && t.text == "and" {
			p.next()
			right, err := p.parseEquality()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: "and", Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseEquality() (*Expr, error) {
	left, err := p.parseCompare()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && (t.text == "==" || t.text == "!=") {
			p.next()
			right, err := p.parseCompare()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: t.text, Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseCompare() (*Expr, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && (t.text == "<" || t.text == ">") {
			p.next()
			right, err := p.parseTerm()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: t.text, Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseTerm() (*Expr, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && (t.text == "+" || t.text == "-") {
			p.next()
			right, err := p.parseFactor()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: t.text, Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseFactor() (*Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == exTokOp && (t.text == "*" || t.text == "/") {
			p.next()
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = &Expr{Kind: exprBinary, Op: t.text, Left: left, Right: right}
			continue
		}
		break
	}
	return left, nil
}

func (p *exprParser) parseUnary() (*Expr, error) {
	t := p.peek()
	if t.kind == exTokOp && (t.text == "not" || t.text == "-") {
		p.next()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &Expr{Kind: exprUnary, Op: t.text, Left: expr}, nil
	}
	return p.parsePostfix()
}

func (p *exprParser) parsePostfix() (*Expr, error) {
	node, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	for {
		switch p.peek().kind {
		case exTokLParen:
			p.next()
			args := []*Expr{}
			if p.peek().kind != exTokRParen {
				for {
					arg, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if p.peek().kind == exTokComma {
						p.next()
						continue
					}
					break
				}
			}
			if err := p.expect(exTokRParen, ""); err != nil {
				return nil, err
			}
			if node.Kind != exprIdent {
				return nil, errors.New("only function identifiers are callable")
			}
			node = &Expr{Kind: exprCall, Name: node.Name, Args: args}
		case exTokLBracket:
			p.next()
			if p.peek().kind == exTokRBracket {
				return nil, errors.New("empty index is not allowed")
			}
			var first *Expr
			var second *Expr
			hasColon := false
			if p.peek().kind != exTokColon {
				first, err = p.parseOr()
				if err != nil {
					return nil, err
				}
			}
			if p.peek().kind == exTokColon {
				hasColon = true
				p.next()
				if p.peek().kind != exTokRBracket {
					second, err = p.parseOr()
					if err != nil {
						return nil, err
					}
				}
			}
			if err := p.expect(exTokRBracket, ""); err != nil {
				return nil, err
			}
			if hasColon {
				node = &Expr{Kind: exprSlice, Target: node, Start: first, End: second}
			} else {
				node = &Expr{Kind: exprIndex, Left: node, Right: first}
			}
		default:
			return node, nil
		}
	}
}

func (p *exprParser) parseAtom() (*Expr, error) {
	t := p.next()
	switch t.kind {
	case exTokNumber:
		i, _ := strconv.Atoi(t.text)
		return &Expr{Kind: exprLiteral, LitKind: "int", I: i}, nil
	case exTokString:
		return &Expr{Kind: exprLiteral, LitKind: "string", S: t.text}, nil
	case exTokIdent:
		switch t.text {
		case "true":
			return &Expr{Kind: exprLiteral, LitKind: "bool", B: true}, nil
		case "false":
			return &Expr{Kind: exprLiteral, LitKind: "bool", B: false}, nil
		case "null":
			return &Expr{Kind: exprLiteral, LitKind: "null"}, nil
		}
		return &Expr{Kind: exprIdent, Name: t.text}, nil
	case exTokLParen:
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(exTokRParen, ""); err != nil {
			return nil, err
		}
		return expr, nil
	case exTokLBracket:
		elems := []*Expr{}
		if p.peek().kind != exTokRBracket {
			for {
				e, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				elems = append(elems, e)
				if p.peek().kind == exTokComma {
					p.next()
					continue
				}
				break
			}
		}
		if err := p.expect(exTokRBracket, ""); err != nil {
			return nil, err
		}
		return &Expr{Kind: exprList, Elements: elems}, nil
	default:
		return nil, fmt.Errorf("unexpected token: %s", t.text)
	}
}

func lexExpr(s string) ([]exprToken, error) {
	toks := []exprToken{}
	for i := 0; i < len(s); {
		r := s[i]
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			i++
			continue
		}
		if r == 'f' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\'') {
			ftoks, next, err := lexFString(s, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, ftoks...)
			i = next
			continue
		}
		if isLetter(r) || r == '_' {
			start := i
			i++
			for i < len(s) && (isLetter(s[i]) || isDigit(s[i]) || s[i] == '_') {
				i++
			}
			text := s[start:i]
			if text == "and" || text == "or" || text == "not" {
				toks = append(toks, exprToken{kind: exTokOp, text: text})
			} else {
				toks = append(toks, exprToken{kind: exTokIdent, text: text})
			}
			continue
		}
		if isDigit(r) {
			start := i
			i++
			for i < len(s) && isDigit(s[i]) {
				i++
			}
			toks = append(toks, exprToken{kind: exTokNumber, text: s[start:i]})
			continue
		}
		if r == '\'' || r == '"' {
			quote := r
			start := i + 1
			i++
			for i < len(s) && s[i] != quote {
				i++
			}
			if i >= len(s) {
				return nil, errors.New("unterminated string")
			}
			text := s[start:i]
			i++
			toks = append(toks, exprToken{kind: exTokString, text: text})
			continue
		}

		switch r {
		case '(':
			toks = append(toks, exprToken{kind: exTokLParen, text: "("})
			i++
		case ')':
			toks = append(toks, exprToken{kind: exTokRParen, text: ")"})
			i++
		case '[':
			toks = append(toks, exprToken{kind: exTokLBracket, text: "["})
			i++
		case ']':
			toks = append(toks, exprToken{kind: exTokRBracket, text: "]"})
			i++
		case ',':
			toks = append(toks, exprToken{kind: exTokComma, text: ","})
			i++
		case ':':
			toks = append(toks, exprToken{kind: exTokColon, text: ":"})
			i++
		case '+', '-', '*', '/', '<', '>':
			toks = append(toks, exprToken{kind: exTokOp, text: string(r)})
			i++
		case '=':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, exprToken{kind: exTokOp, text: "=="})
				i += 2
			} else {
				return nil, errors.New("unexpected '='")
			}
		case '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, exprToken{kind: exTokOp, text: "!="})
				i += 2
			} else {
				return nil, errors.New("unexpected '!'")
			}
		default:
			return nil, fmt.Errorf("unexpected character: %c", r)
		}
	}
	toks = append(toks, exprToken{kind: exTokEOF})
	return toks, nil
}

func lexFString(s string, start int) ([]exprToken, int, error) {
	if start+1 >= len(s) {
		return nil, start, errors.New("unterminated f-string")
	}
	quote := s[start+1]
	i := start + 2
	parts := [][]exprToken{}
	var lit strings.Builder
	foundPart := false

	for i < len(s) {
		ch := s[i]
		if ch == quote {
			i++
			break
		}
		if ch == '{' {
			if lit.Len() > 0 {
				parts = append(parts, []exprToken{{kind: exTokString, text: lit.String()}})
				lit.Reset()
				foundPart = true
			}
			end := i + 1
			depth := 1
			for end < len(s) {
				if s[end] == '{' {
					depth++
				} else if s[end] == '}' {
					depth--
					if depth == 0 {
						break
					}
				}
				end++
			}
			if end >= len(s) || s[end] != '}' {
				return nil, start, errors.New("unterminated f-string expression")
			}
			inner := strings.TrimSpace(s[i+1 : end])
			if inner == "" {
				return nil, start, errors.New("empty f-string expression")
			}
			exprTokens, err := lexExpr(inner)
			if err != nil {
				return nil, start, err
			}
			if len(exprTokens) > 0 {
				exprTokens = exprTokens[:len(exprTokens)-1]
			}
			part := []exprToken{{kind: exTokLParen, text: "("}}
			part = append(part, exprTokens...)
			part = append(part, exprToken{kind: exTokRParen, text: ")"})
			parts = append(parts, part)
			foundPart = true
			i = end + 1
			continue
		}
		lit.WriteByte(ch)
		i++
	}

	if i >= len(s) && (len(s) == 0 || s[i-1] != quote) {
		return nil, start, errors.New("unterminated f-string")
	}
	if lit.Len() > 0 {
		parts = append(parts, []exprToken{{kind: exTokString, text: lit.String()}})
		foundPart = true
	}
	if !foundPart {
		parts = append(parts, []exprToken{{kind: exTokString, text: ""}})
	}

	out := []exprToken{}
	for idx, p := range parts {
		if idx > 0 {
			out = append(out, exprToken{kind: exTokOp, text: "+"})
		}
		out = append(out, p...)
	}
	return out, i, nil
}

func isLetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func (c *Compiler) emitLoad(name string, code *[]Instr, ctx *compileCtx, lineNo int) error {
	if ctx != nil && ctx.inFunction && ctx.scope != nil {
		if slot, ok := ctx.scope.resolve(name); ok {
			*code = append(*code, Instr{Op: OP_LOAD_LOCAL, Int: slot, Line: lineNo})
			return nil
		}
	}
	*code = append(*code, Instr{Op: OP_LOAD, Str: name, Line: lineNo})
	return nil
}

func (c *Compiler) emitStore(name string, code *[]Instr, ctx *compileCtx, lineNo int) error {
	if ctx != nil && ctx.inFunction && ctx.scope != nil {
		slot, ok := ctx.scope.resolve(name)
		if !ok {
			slot = ctx.defineLocal(name)
		}
		*code = append(*code, Instr{Op: OP_STORE_LOCAL, Int: slot, Line: lineNo})
		return nil
	}
	*code = append(*code, Instr{Op: OP_STORE, Str: name, Line: lineNo})
	return nil
}

func (c *Compiler) compileExpr(expr string, code *[]Instr, types *typeEnv, ctx *compileCtx, lineNo int) error {
	root, err := parseExprAst(expr)
	if err != nil {
		return err
	}
	if types != nil {
		if _, err := c.inferExprType(root, types); err != nil {
			return err
		}
	}
	return c.compileExprNode(root, code, types, ctx, lineNo)
}

func (c *Compiler) predeclareFunctions() error {
	for _, t := range c.tokens {
		if t.Kind != tokLine {
			continue
		}
		if !strings.HasPrefix(t.Text, "fnc ") {
			continue
		}
		name, params, paramTypes, retType, err := parseFnDecl(t.Text)
		if err != nil {
			return fmt.Errorf("line %d: %w", t.Line, err)
		}
		if _, exists := c.functions[name]; exists {
			return fmt.Errorf("line %d: duplicate function name: %s", t.Line, name)
		}
		c.functions[name] = Function{Params: params, ParamTypes: paramTypes, ReturnType: retType}
	}
	return nil
}

func parseExprAst(expr string) (*Expr, error) {
	toks, err := lexExpr(expr)
	if err != nil {
		return nil, err
	}
	p := &exprParser{tokens: toks}
	root, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != exTokEOF {
		return nil, errors.New("unexpected trailing tokens")
	}
	return root, nil
}

func (c *Compiler) inferExprType(node *Expr, types *typeEnv) (ValueType, error) {
	switch node.Kind {
	case exprLiteral:
		switch node.LitKind {
		case "int":
			return typeInt, nil
		case "string":
			return typeString, nil
		case "bool":
			return typeBool, nil
		case "null":
			return typeNull, nil
		default:
			return typeUnknown, nil
		}
	case exprIdent:
		if types == nil {
			return typeUnknown, nil
		}
		if v, ok := types.get(node.Name); ok {
			return v, nil
		}
		return typeUnknown, nil
	case exprUnary:
		inner, err := c.inferExprType(node.Left, types)
		if err != nil {
			return typeUnknown, err
		}
		switch node.Op {
		case "not":
			return typeBool, nil
		case "-":
			if inner != typeUnknown && inner != typeInt {
				return typeUnknown, errors.New("unary '-' expects int")
			}
			if inner == typeUnknown {
				return typeUnknown, nil
			}
			return typeInt, nil
		default:
			return typeUnknown, fmt.Errorf("unsupported unary op: %s", node.Op)
		}
	case exprBinary:
		lt, err := c.inferExprType(node.Left, types)
		if err != nil {
			return typeUnknown, err
		}
		rt, err := c.inferExprType(node.Right, types)
		if err != nil {
			return typeUnknown, err
		}
		switch node.Op {
		case "+":
			if lt != typeUnknown && rt != typeUnknown {
				if lt == typeInt && rt == typeInt {
					return typeInt, nil
				}
				if lt == typeList && rt == typeList {
					return typeList, nil
				}
				if lt == typeString || rt == typeString {
					return typeString, nil
				}
				return typeUnknown, errors.New("'+' expects int, string, or list")
			}
			return typeUnknown, nil
		case "-":
			if lt != typeUnknown && lt != typeInt {
				return typeUnknown, errors.New("'-' expects int")
			}
			if rt != typeUnknown && rt != typeInt {
				return typeUnknown, errors.New("'-' expects int")
			}
			if lt == typeInt && rt == typeInt {
				return typeInt, nil
			}
			return typeUnknown, nil
		case "*":
			if lt != typeUnknown && lt != typeInt {
				return typeUnknown, errors.New("'*' expects int")
			}
			if rt != typeUnknown && rt != typeInt {
				return typeUnknown, errors.New("'*' expects int")
			}
			if lt == typeInt && rt == typeInt {
				return typeInt, nil
			}
			return typeUnknown, nil
		case "/":
			if lt != typeUnknown && lt != typeInt {
				return typeUnknown, errors.New("'/' expects int")
			}
			if rt != typeUnknown && rt != typeInt {
				return typeUnknown, errors.New("'/' expects int")
			}
			if lt == typeInt && rt == typeInt {
				return typeInt, nil
			}
			return typeUnknown, nil
		case "==", "!=":
			return typeBool, nil
		case "<", ">":
			if lt != typeUnknown && rt != typeUnknown {
				if lt == typeInt && rt == typeInt {
					return typeBool, nil
				}
				if lt == typeString && rt == typeString {
					return typeBool, nil
				}
				return typeUnknown, errors.New("'<' and '>' expect int or string")
			}
			return typeBool, nil
		case "and", "or":
			return typeBool, nil
		default:
			return typeUnknown, fmt.Errorf("unsupported op: %s", node.Op)
		}
	case exprCall:
		switch node.Name {
		case "input":
			if len(node.Args) > 1 {
				return typeUnknown, errors.New("input expects 0 or 1 argument")
			}
			return typeString, nil
		case "plc":
			if len(node.Args) != 1 {
				return typeUnknown, errors.New("plc expects 1 argument")
			}
			return typeString, nil
		case "read":
			if len(node.Args) > 1 {
				return typeUnknown, errors.New("read expects 0 or 1 argument")
			}
			return typeString, nil
		case "len":
			if len(node.Args) != 1 {
				return typeUnknown, errors.New("len expects 1 argument")
			}
			return typeInt, nil
		case "append":
			if len(node.Args) != 2 {
				return typeUnknown, errors.New("append expects 2 arguments")
			}
			lt, err := c.inferExprType(node.Args[0], types)
			if err != nil {
				return typeUnknown, err
			}
			if lt != typeUnknown && lt != typeList {
				return typeUnknown, errors.New("append expects list as first argument")
			}
			return typeList, nil
		case "keys":
			if len(node.Args) != 1 {
				return typeUnknown, errors.New("keys expects 1 argument")
			}
			dt, err := c.inferExprType(node.Args[0], types)
			if err != nil {
				return typeUnknown, err
			}
			if dt != typeUnknown && dt != typeDict {
				return typeUnknown, errors.New("keys expects dict")
			}
			return typeList, nil
		case "math_abs":
			if len(node.Args) != 1 {
				return typeUnknown, errors.New("math_abs expects 1 argument")
			}
			return typeInt, nil
		case "math_min", "math_max", "math_pow":
			if len(node.Args) != 2 {
				return typeUnknown, errors.New(node.Name + " expects 2 arguments")
			}
			return typeInt, nil
		case "table", "dict":
			return typeDict, nil
		default:
			fn, ok := c.functions[node.Name]
			if !ok {
				return typeUnknown, nil
			}
			if len(node.Args) != len(fn.Params) {
				return typeUnknown, fmt.Errorf("function %s expects %d args, got %d", node.Name, len(fn.Params), len(node.Args))
			}
			for i, arg := range node.Args {
				at, err := c.inferExprType(arg, types)
				if err != nil {
					return typeUnknown, err
				}
				if i < len(fn.ParamTypes) && fn.ParamTypes[i] != typeUnknown {
					if at == typeUnknown {
						return typeUnknown, fmt.Errorf("type mismatch: %s arg %d is %s, got %s", node.Name, i+1, fn.ParamTypes[i].String(), at.String())
					}
					if !assignable(fn.ParamTypes[i], at) {
						return typeUnknown, fmt.Errorf("type mismatch: %s arg %d is %s, got %s", node.Name, i+1, fn.ParamTypes[i].String(), at.String())
					}
				}
			}
			if fn.ReturnType == typeUnknown {
				return typeUnknown, nil
			}
			return fn.ReturnType, nil
		}
	case exprList:
		return typeList, nil
	case exprIndex:
		targetType, err := c.inferExprType(node.Left, types)
		if err != nil {
			return typeUnknown, err
		}
		if targetType == typeList {
			return typeUnknown, nil
		}
		if targetType == typeString {
			return typeString, nil
		}
		if targetType == typeDict {
			return typeUnknown, nil
		}
		return typeUnknown, nil
	case exprSlice:
		targetType, err := c.inferExprType(node.Target, types)
		if err != nil {
			return typeUnknown, err
		}
		if targetType != typeUnknown && targetType != typeList && targetType != typeString {
			return typeUnknown, errors.New("slice expects list or string")
		}
		if targetType == typeString {
			return typeString, nil
		}
		if targetType == typeList {
			return typeList, nil
		}
		return typeUnknown, nil
	default:
		return typeUnknown, errors.New("unknown expr")
	}
}

func (c *Compiler) compileExprNode(node *Expr, code *[]Instr, types *typeEnv, ctx *compileCtx, lineNo int) error {
	switch node.Kind {
	case exprLiteral:
		switch node.LitKind {
		case "int":
			*code = append(*code, Instr{Op: OP_PUSH_INT, Int: node.I, Line: lineNo})
		case "string":
			*code = append(*code, Instr{Op: OP_PUSH_STR, Str: node.S, Line: lineNo})
		case "bool":
			*code = append(*code, Instr{Op: OP_PUSH_BOOL, Bool: node.B, Line: lineNo})
		case "null":
			*code = append(*code, Instr{Op: OP_PUSH_NULL, Line: lineNo})
		default:
			return errors.New("unknown literal")
		}
		return nil
	case exprIdent:
		return c.emitLoad(node.Name, code, ctx, lineNo)
	case exprUnary:
		if err := c.compileExprNode(node.Left, code, types, ctx, lineNo); err != nil {
			return err
		}
		switch node.Op {
		case "not":
			*code = append(*code, Instr{Op: OP_NOT, Line: lineNo})
		case "-":
			*code = append(*code, Instr{Op: OP_NEG, Line: lineNo})
		default:
			return fmt.Errorf("unsupported unary op: %s", node.Op)
		}
		return nil
	case exprBinary:
		if err := c.compileExprNode(node.Left, code, types, ctx, lineNo); err != nil {
			return err
		}
		if err := c.compileExprNode(node.Right, code, types, ctx, lineNo); err != nil {
			return err
		}
		switch node.Op {
		case "+":
			*code = append(*code, Instr{Op: OP_ADD, Line: lineNo})
		case "-":
			*code = append(*code, Instr{Op: OP_SUB, Line: lineNo})
		case "*":
			*code = append(*code, Instr{Op: OP_MUL, Line: lineNo})
		case "/":
			*code = append(*code, Instr{Op: OP_DIV, Line: lineNo})
		case "==":
			*code = append(*code, Instr{Op: OP_EQ, Line: lineNo})
		case "!=":
			*code = append(*code, Instr{Op: OP_NEQ, Line: lineNo})
		case "<":
			*code = append(*code, Instr{Op: OP_LT, Line: lineNo})
		case ">":
			*code = append(*code, Instr{Op: OP_GT, Line: lineNo})
		case "and":
			*code = append(*code, Instr{Op: OP_AND, Line: lineNo})
		case "or":
			*code = append(*code, Instr{Op: OP_OR, Line: lineNo})
		default:
			return fmt.Errorf("unsupported op: %s", node.Op)
		}
		return nil
	case exprCall:
		for _, arg := range node.Args {
			if err := c.compileExprNode(arg, code, types, ctx, lineNo); err != nil {
				return err
			}
		}
		*code = append(*code, Instr{Op: OP_CALL, Str: node.Name, Int: len(node.Args), Line: lineNo})
		return nil
	case exprList:
		for _, e := range node.Elements {
			if err := c.compileExprNode(e, code, types, ctx, lineNo); err != nil {
				return err
			}
		}
		*code = append(*code, Instr{Op: OP_MAKE_LIST, Int: len(node.Elements), Line: lineNo})
		return nil
	case exprIndex:
		if err := c.compileExprNode(node.Left, code, types, ctx, lineNo); err != nil {
			return err
		}
		if err := c.compileExprNode(node.Right, code, types, ctx, lineNo); err != nil {
			return err
		}
		*code = append(*code, Instr{Op: OP_INDEX_GET, Line: lineNo})
		return nil
	case exprSlice:
		if err := c.compileExprNode(node.Target, code, types, ctx, lineNo); err != nil {
			return err
		}
		if node.Start != nil {
			if err := c.compileExprNode(node.Start, code, types, ctx, lineNo); err != nil {
				return err
			}
		} else {
			*code = append(*code, Instr{Op: OP_PUSH_NULL, Line: lineNo})
		}
		if node.End != nil {
			if err := c.compileExprNode(node.End, code, types, ctx, lineNo); err != nil {
				return err
			}
		} else {
			*code = append(*code, Instr{Op: OP_PUSH_NULL, Line: lineNo})
		}
		*code = append(*code, Instr{Op: OP_SLICE, Line: lineNo})
		return nil
	default:
		return errors.New("unknown expr")
	}
}

func parseStringLiteral(s string) (string, bool) {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1], true
	}
	return "", false
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
				return false
			}
			continue
		}
		if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

type Value struct {
	Kind string
	I    int
	S    string
	B    bool
	L    []Value
	M    map[string]Value
}

type VM struct {
	globals map[string]Value
	reader  *bufio.Reader
}

type traceFrame struct {
	Function string
	Line     int
}

type RuntimeError struct {
	Message string
	Frames  []traceFrame
}

func (e *RuntimeError) Error() string {
	var b strings.Builder
	b.WriteString(e.Message)
	if len(e.Frames) > 0 {
		b.WriteString("\nTraceback:")
		for i := 0; i < len(e.Frames); i++ {
			f := e.Frames[i]
			if f.Line > 0 {
				b.WriteString(fmt.Sprintf("\n  at %s (line %d)", f.Function, f.Line))
			} else {
				b.WriteString(fmt.Sprintf("\n  at %s", f.Function))
			}
		}
	}
	return b.String()
}

func wrapRuntime(err error, fnName string, line int) error {
	if err == nil {
		return nil
	}
	if rt, ok := err.(*RuntimeError); ok {
		rt.Frames = append(rt.Frames, traceFrame{Function: fnName, Line: line})
		return rt
	}
	return &RuntimeError{
		Message: err.Error(),
		Frames:  []traceFrame{{Function: fnName, Line: line}},
	}
}

func NewVM() *VM {
	return &VM{globals: map[string]Value{}, reader: bufio.NewReader(os.Stdin)}
}

func (vm *VM) Exec(prog *Program, code []Instr) (Value, error) {
	return vm.execWithLocals(prog, code, nil, "<main>")
}

func (vm *VM) execWithLocals(prog *Program, code []Instr, locals []Value, fnName string) (Value, error) {
	stack := []Value{}
	pc := 0
	fail := func(err error, ins Instr) (Value, error) {
		return Value{}, wrapRuntime(err, fnName, ins.Line)
	}
	for pc < len(code) {
		ins := code[pc]
		switch ins.Op {
		case OP_PUSH_INT:
			stack = append(stack, Value{Kind: "int", I: ins.Int})
		case OP_PUSH_STR:
			stack = append(stack, Value{Kind: "string", S: ins.Str})
		case OP_PUSH_BOOL:
			stack = append(stack, Value{Kind: "bool", B: ins.Bool})
		case OP_PUSH_NULL:
			stack = append(stack, Value{Kind: "null"})
		case OP_LOAD:
			v, ok := vm.globals[ins.Str]
			if !ok {
				v = Value{Kind: "null"}
			}
			stack = append(stack, v)
		case OP_LOAD_LOCAL:
			if ins.Int < 0 || ins.Int >= len(locals) {
				return fail(fmt.Errorf("invalid local slot: %d", ins.Int), ins)
			}
			stack = append(stack, locals[ins.Int])
		case OP_STORE:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			vm.globals[ins.Str] = v
		case OP_STORE_LOCAL:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			if ins.Int < 0 || ins.Int >= len(locals) {
				return fail(fmt.Errorf("invalid local slot: %d", ins.Int), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			locals[ins.Int] = v
		case OP_PRINT:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			fmt.Println(v.String())
		case OP_INPUT:
			fmt.Print(ins.Str)
			text, _ := vm.reader.ReadString('\n')
			text = strings.TrimRight(text, "\r\n")
			v := Value{Kind: "string", S: text}
			stack = append(stack, v)
		case OP_WAIT:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			ms := v.AsInt()
			time.Sleep(time.Duration(ms) * time.Millisecond)
		case OP_CHECK:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !v.Truthy() {
				return fail(fmt.Errorf("check failed: %s", ins.Str), ins)
			}
		case OP_JMP:
			pc = ins.Int
			continue
		case OP_JMP_IF_FALSE:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !v.Truthy() {
				pc = ins.Int
				continue
			}
		case OP_CALL:
			args, err := popArgs(&stack, ins.Int)
			if err != nil {
				return fail(err, ins)
			}
			if builtin, ok, err := vm.callBuiltin(ins.Str, args); ok {
				if err != nil {
					return fail(err, ins)
				}
				stack = append(stack, builtin)
				break
			}
			fn, ok := prog.Functions[ins.Str]
			if !ok {
				return fail(fmt.Errorf("unknown function: %s", ins.Str), ins)
			}
			size := fn.LocalCount
			if size < len(fn.Params) {
				size = len(fn.Params)
			}
			localsFrame := make([]Value, size)
			for i := range localsFrame {
				localsFrame[i] = Value{Kind: "null"}
			}
			for i := range fn.Params {
				if i < len(args) {
					localsFrame[i] = args[i]
				}
			}
			ret, err := vm.execWithLocals(prog, fn.Code, localsFrame, ins.Str)
			if err != nil {
				return Value{}, wrapRuntime(err, fnName, ins.Line)
			}
			stack = append(stack, ret)
		case OP_RET:
			if len(stack) == 0 {
				return Value{Kind: "null"}, nil
			}
			v := stack[len(stack)-1]
			return v, nil
		case OP_POP:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			stack = stack[:len(stack)-1]
		case OP_ADD:
			if err := binOp(&stack, opAdd); err != nil {
				return fail(err, ins)
			}
		case OP_SUB:
			if err := binOp(&stack, opSub); err != nil {
				return fail(err, ins)
			}
		case OP_MUL:
			if err := binOp(&stack, opMul); err != nil {
				return fail(err, ins)
			}
		case OP_DIV:
			if err := binOp(&stack, opDiv); err != nil {
				return fail(err, ins)
			}
		case OP_EQ:
			if err := binOp(&stack, opEq); err != nil {
				return fail(err, ins)
			}
		case OP_NEQ:
			if err := binOp(&stack, opNeq); err != nil {
				return fail(err, ins)
			}
		case OP_LT:
			if err := binOp(&stack, opLt); err != nil {
				return fail(err, ins)
			}
		case OP_GT:
			if err := binOp(&stack, opGt); err != nil {
				return fail(err, ins)
			}
		case OP_AND:
			if err := binOp(&stack, opAnd); err != nil {
				return fail(err, ins)
			}
		case OP_OR:
			if err := binOp(&stack, opOr); err != nil {
				return fail(err, ins)
			}
		case OP_NOT:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			stack = append(stack, Value{Kind: "bool", B: !v.Truthy()})
		case OP_NEG:
			if len(stack) == 0 {
				return fail(errors.New("stack underflow"), ins)
			}
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if v.Kind != "int" {
				return fail(errors.New("unary '-' expects int"), ins)
			}
			stack = append(stack, Value{Kind: "int", I: -v.I})
		case OP_MAKE_LIST:
			if len(stack) < ins.Int {
				return fail(errors.New("stack underflow"), ins)
			}
			items := make([]Value, ins.Int)
			for i := ins.Int - 1; i >= 0; i-- {
				items[i] = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}
			stack = append(stack, Value{Kind: "list", L: items})
		case OP_INDEX_GET:
			if len(stack) < 2 {
				return fail(errors.New("stack underflow"), ins)
			}
			idx := stack[len(stack)-1]
			target := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			v, err := indexGet(target, idx)
			if err != nil {
				return fail(err, ins)
			}
			stack = append(stack, v)
		case OP_INDEX_SET:
			if len(stack) < 3 {
				return fail(errors.New("stack underflow"), ins)
			}
			val := stack[len(stack)-1]
			idx := stack[len(stack)-2]
			target := stack[len(stack)-3]
			stack = stack[:len(stack)-3]
			if err := indexSet(target, idx, val); err != nil {
				return fail(err, ins)
			}
		case OP_SLICE:
			if len(stack) < 3 {
				return fail(errors.New("stack underflow"), ins)
			}
			end := stack[len(stack)-1]
			start := stack[len(stack)-2]
			target := stack[len(stack)-3]
			stack = stack[:len(stack)-3]
			v, err := doSlice(target, start, end)
			if err != nil {
				return fail(err, ins)
			}
			stack = append(stack, v)
		case OP_NOOP:
			// no-op
		default:
			return fail(errors.New("unknown opcode"), ins)
		}
		pc++
	}
	return Value{Kind: "null"}, nil
}

func (vm *VM) callBuiltin(name string, args []Value) (Value, bool, error) {
	switch name {
	case "table", "dict":
		if len(args)%2 != 0 {
			return Value{}, true, errors.New("dict expects key/value pairs")
		}
		m := map[string]Value{}
		for i := 0; i < len(args); i += 2 {
			key := args[i]
			k := key.String()
			m[k] = args[i+1]
		}
		return Value{Kind: "dict", M: m}, true, nil
	case "len":
		if len(args) != 1 {
			return Value{}, true, errors.New("len expects 1 argument")
		}
		switch args[0].Kind {
		case "string":
			return Value{Kind: "int", I: len(args[0].S)}, true, nil
		case "list":
			return Value{Kind: "int", I: len(args[0].L)}, true, nil
		case "dict":
			return Value{Kind: "int", I: len(args[0].M)}, true, nil
		default:
			return Value{}, true, errors.New("len expects string, list, or dict")
		}
	case "append":
		if len(args) != 2 {
			return Value{}, true, errors.New("append expects 2 arguments")
		}
		if args[0].Kind != "list" {
			return Value{}, true, errors.New("append expects list as first argument")
		}
		out := append([]Value{}, args[0].L...)
		out = append(out, args[1])
		return Value{Kind: "list", L: out}, true, nil
	case "keys":
		if len(args) != 1 {
			return Value{}, true, errors.New("keys expects 1 argument")
		}
		if args[0].Kind != "dict" {
			return Value{}, true, errors.New("keys expects dict")
		}
		items := make([]Value, 0, len(args[0].M))
		for k := range args[0].M {
			items = append(items, Value{Kind: "string", S: k})
		}
		return Value{Kind: "list", L: items}, true, nil
	case "input", "read":
		if len(args) > 1 {
			return Value{}, true, errors.New("input expects 0 or 1 argument")
		}
		prompt := ""
		if len(args) == 1 {
			prompt = args[0].String()
		}
		fmt.Print(prompt)
		text, _ := vm.reader.ReadString('\n')
		text = strings.TrimRight(text, "\r\n")
		return Value{Kind: "string", S: text}, true, nil
	case "plc":
		if len(args) != 1 {
			return Value{}, true, errors.New("plc expects 1 argument")
		}
		return Value{Kind: "string", S: args[0].String()}, true, nil
	case "math_abs":
		if len(args) != 1 {
			return Value{}, true, errors.New("math_abs expects 1 argument")
		}
		return Value{Kind: "int", I: absInt(args[0].AsInt())}, true, nil
	case "math_min":
		if len(args) != 2 {
			return Value{}, true, errors.New("math_min expects 2 arguments")
		}
		a, b := args[0].AsInt(), args[1].AsInt()
		if a < b {
			return Value{Kind: "int", I: a}, true, nil
		}
		return Value{Kind: "int", I: b}, true, nil
	case "math_max":
		if len(args) != 2 {
			return Value{}, true, errors.New("math_max expects 2 arguments")
		}
		a, b := args[0].AsInt(), args[1].AsInt()
		if a > b {
			return Value{Kind: "int", I: a}, true, nil
		}
		return Value{Kind: "int", I: b}, true, nil
	case "math_pow":
		if len(args) != 2 {
			return Value{}, true, errors.New("math_pow expects 2 arguments")
		}
		a, b := float64(args[0].AsInt()), float64(args[1].AsInt())
		return Value{Kind: "int", I: int(math.Pow(a, b))}, true, nil
	default:
		return Value{}, false, nil
	}
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func indexGet(target Value, idx Value) (Value, error) {
	switch target.Kind {
	case "list":
		i, err := resolveIndex(idx, len(target.L))
		if err != nil {
			return Value{}, err
		}
		return target.L[i], nil
	case "dict":
		k := idx.String()
		if v, ok := target.M[k]; ok {
			return v, nil
		}
		return Value{Kind: "null"}, nil
	case "string":
		i, err := resolveIndex(idx, len(target.S))
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: "string", S: string(target.S[i])}, nil
	default:
		return Value{}, errors.New("indexing expects list, dict, or string")
	}
}

func indexSet(target Value, idx Value, val Value) error {
	switch target.Kind {
	case "list":
		i, err := resolveIndex(idx, len(target.L))
		if err != nil {
			return err
		}
		target.L[i] = val
		return nil
	case "dict":
		k := idx.String()
		target.M[k] = val
		return nil
	default:
		return errors.New("index assignment expects list or dict")
	}
}

func doSlice(target Value, start Value, end Value) (Value, error) {
	switch target.Kind {
	case "list":
		s, e, err := resolveSliceBounds(len(target.L), start, end)
		if err != nil {
			return Value{}, err
		}
		out := append([]Value{}, target.L[s:e]...)
		return Value{Kind: "list", L: out}, nil
	case "string":
		s, e, err := resolveSliceBounds(len(target.S), start, end)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: "string", S: target.S[s:e]}, nil
	default:
		return Value{}, errors.New("slice expects list or string")
	}
}

func resolveIndex(v Value, length int) (int, error) {
	if v.Kind != "int" {
		return 0, errors.New("index must be int")
	}
	i := v.I
	if i < 0 {
		i = length + i
	}
	if i < 0 || i >= length {
		return 0, fmt.Errorf("index out of range: %d", v.I)
	}
	return i, nil
}

func resolveSliceBounds(length int, start Value, end Value) (int, int, error) {
	s := 0
	e := length
	if start.Kind != "null" {
		if start.Kind != "int" {
			return 0, 0, errors.New("slice start must be int")
		}
		s = start.I
		if s < 0 {
			s = length + s
		}
	}
	if end.Kind != "null" {
		if end.Kind != "int" {
			return 0, 0, errors.New("slice end must be int")
		}
		e = end.I
		if e < 0 {
			e = length + e
		}
	}
	if s < 0 {
		s = 0
	}
	if e > length {
		e = length
	}
	if s > e {
		s = e
	}
	return s, e, nil
}

func popArgs(stack *[]Value, n int) ([]Value, error) {
	if len(*stack) < n {
		return nil, errors.New("stack underflow")
	}
	args := make([]Value, n)
	for i := n - 1; i >= 0; i-- {
		args[i] = (*stack)[len(*stack)-1]
		*stack = (*stack)[:len(*stack)-1]
	}
	return args, nil
}

func binOp(stack *[]Value, fn func(Value, Value) (Value, error)) error {
	if len(*stack) < 2 {
		return errors.New("stack underflow")
	}
	r := (*stack)[len(*stack)-1]
	l := (*stack)[len(*stack)-2]
	*stack = (*stack)[:len(*stack)-2]
	v, err := fn(l, r)
	if err != nil {
		return err
	}
	*stack = append(*stack, v)
	return nil
}

func opAdd(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		return Value{Kind: "int", I: a.I + b.I}, nil
	}
	if a.Kind == "string" || b.Kind == "string" {
		return Value{Kind: "string", S: a.String() + b.String()}, nil
	}
	if a.Kind == "list" && b.Kind == "list" {
		out := append([]Value{}, a.L...)
		out = append(out, b.L...)
		return Value{Kind: "list", L: out}, nil
	}
	return Value{}, errors.New("'+' expects int, string, or list")
}

func opSub(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		return Value{Kind: "int", I: a.I - b.I}, nil
	}
	return Value{}, errors.New("'-' expects int")
}

func opMul(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		return Value{Kind: "int", I: a.I * b.I}, nil
	}
	return Value{}, errors.New("'*' expects int")
}

func opDiv(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		if b.I == 0 {
			return Value{}, errors.New("division by zero")
		}
		return Value{Kind: "int", I: a.I / b.I}, nil
	}
	return Value{}, errors.New("'/' expects int")
}

func opEq(a, b Value) (Value, error) {
	return Value{Kind: "bool", B: a.Equal(b)}, nil
}

func opNeq(a, b Value) (Value, error) {
	return Value{Kind: "bool", B: !a.Equal(b)}, nil
}

func opLt(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		return Value{Kind: "bool", B: a.I < b.I}, nil
	}
	if a.Kind == "string" && b.Kind == "string" {
		return Value{Kind: "bool", B: a.S < b.S}, nil
	}
	return Value{}, errors.New("'<' expects int or string")
}

func opGt(a, b Value) (Value, error) {
	if a.Kind == "int" && b.Kind == "int" {
		return Value{Kind: "bool", B: a.I > b.I}, nil
	}
	if a.Kind == "string" && b.Kind == "string" {
		return Value{Kind: "bool", B: a.S > b.S}, nil
	}
	return Value{}, errors.New("'>' expects int or string")
}

func opAnd(a, b Value) (Value, error) {
	return Value{Kind: "bool", B: a.Truthy() && b.Truthy()}, nil
}

func opOr(a, b Value) (Value, error) {
	return Value{Kind: "bool", B: a.Truthy() || b.Truthy()}, nil
}

func (v Value) Truthy() bool {
	switch v.Kind {
	case "null":
		return false
	case "bool":
		return v.B
	case "int":
		return v.I != 0
	case "string":
		return v.S != ""
	case "list":
		return len(v.L) > 0
	case "dict":
		return len(v.M) > 0
	default:
		return false
	}
}

func (v Value) AsInt() int {
	switch v.Kind {
	case "int":
		return v.I
	case "bool":
		if v.B {
			return 1
		}
		return 0
	case "string":
		if i, err := strconv.Atoi(v.S); err == nil {
			return i
		}
	}
	return 0
}

func (v Value) Equal(o Value) bool {
	if v.Kind != o.Kind {
		return false
	}
	switch v.Kind {
	case "null":
		return true
	case "bool":
		return v.B == o.B
	case "int":
		return v.I == o.I
	case "string":
		return v.S == o.S
	case "list":
		if len(v.L) != len(o.L) {
			return false
		}
		for i := range v.L {
			if !v.L[i].Equal(o.L[i]) {
				return false
			}
		}
		return true
	case "dict":
		if len(v.M) != len(o.M) {
			return false
		}
		for k, vv := range v.M {
			ov, ok := o.M[k]
			if !ok || !vv.Equal(ov) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (v Value) String() string {
	switch v.Kind {
	case "null":
		return "null"
	case "bool":
		if v.B {
			return "true"
		}
		return "false"
	case "int":
		return strconv.Itoa(v.I)
	case "string":
		return v.S
	case "list":
		parts := make([]string, 0, len(v.L))
		for _, it := range v.L {
			parts = append(parts, it.String())
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case "dict":
		parts := []string{}
		for k, val := range v.M {
			parts = append(parts, k+": "+val.String())
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return ""
	}
}

func inputPromptFromCall(node *Expr) (string, error) {
	if node.Kind != exprCall || node.Name != "input" {
		return "", errors.New("invalid input call")
	}
	if len(node.Args) != 1 {
		return "", errors.New("input expects 1 argument")
	}
	arg := node.Args[0]
	if arg.Kind != exprCall || arg.Name != "plc" {
		return "", errors.New("input expects plc(\"prompt\")")
	}
	if len(arg.Args) != 1 {
		return "", errors.New("plc expects 1 argument")
	}
	p := arg.Args[0]
	if p.Kind != exprLiteral || p.LitKind != "string" {
		return "", errors.New("plc expects a string literal")
	}
	return p.S, nil
}

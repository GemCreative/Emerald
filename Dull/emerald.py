import re
import sys
import ast
import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# -------------------------
# Tokenizer
# -------------------------
TOKEN_REGEX = re.compile(
    r"""
    (?P<NUMBER>\d+(\.\d+)?) |
    (?P<STRING>"([^"\\]|\\.)*"|'([^'\\]|\\.)*') |
    (?P<NAME>[A-Za-z_][A-Za-z0-9_]*) |
    (?P<OP>[+\-*/%=<>!]+) |
    (?P<NEWLINE>\n) |
    (?P<SKIP>[ \t]+) |
    (?P<COMMENT>--.*) |
    (?P<SYMBOL>[{}()\[\],.:]) 
    """,
    re.VERBOSE,
)

KEYWORDS = {
    "var", "const", "func", "class", "new", "if", "else", "while", "repeat",
    "return", "break", "continue", "await", "true", "false", "null", "this"
}

@dataclass
class Token:
    type: str
    value: str

def tokenize(code: str) -> List[Token]:
    tokens = []
    for m in TOKEN_REGEX.finditer(code):
        if m.lastgroup == "SKIP" or m.lastgroup == "COMMENT":
            continue
        tok_type = m.lastgroup
        tok_val = m.group(0)
        if tok_type == "NAME" and tok_val in KEYWORDS:
            tok_type = tok_val.upper()
        tokens.append(Token(tok_type, tok_val))
    return tokens

# -------------------------
# Parser
# -------------------------
@dataclass
class Node: pass

@dataclass
class Program(Node):
    statements: List[Node]

@dataclass
class VarDecl(Node):
    name: str
    expr: Node
    const: bool = False

@dataclass
class Assign(Node):
    target: Node
    expr: Node

@dataclass
class If(Node):
    cond: Node
    then_block: List[Node]
    elif_blocks: List[Tuple[Node, List[Node]]]
    else_block: Optional[List[Node]]

@dataclass
class While(Node):
    cond: Node
    block: List[Node]

@dataclass
class Repeat(Node):
    times: Node
    block: List[Node]

@dataclass
class FuncDef(Node):
    name: str
    params: List[str]
    block: List[Node]

@dataclass
class Return(Node):
    expr: Optional[Node]

@dataclass
class Break(Node): pass
@dataclass
class Continue(Node): pass

@dataclass
class Call(Node):
    callee: Node
    args: List[Node]

@dataclass
class ExprStmt(Node):
    expr: Node

@dataclass
class Literal(Node):
    value: Any

@dataclass
class Var(Node):
    name: str

@dataclass
class BinOp(Node):
    left: Node
    op: str
    right: Node

@dataclass
class UnaryOp(Node):
    op: str
    expr: Node

@dataclass
class Array(Node):
    items: List[Node]

@dataclass
class DictLit(Node):
    items: List[Tuple[Node, Node]]

@dataclass
class Index(Node):
    obj: Node
    key: Node

@dataclass
class Attr(Node):
    obj: Node
    name: str

@dataclass
class ClassDef(Node):
    name: str
    base: Optional[str]
    props: List[Node]  # VarDecl + FuncDef inside class

@dataclass
class New(Node):
    class_name: str
    var_name: str

@dataclass
class Await(Node):
    expr: Node

class Parser:
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.i = 0

    def peek(self, offset=0) -> Optional[Token]:
        if self.i + offset < len(self.tokens):
            return self.tokens[self.i + offset]
        return None

    def eat(self, type_: str, value: Optional[str] = None) -> Token:
        tok = self.peek()
        if not tok or tok.type != type_ or (value is not None and tok.value != value):
            raise SyntaxError(f"Expected {type_} {value or ''} but got {tok}")
        self.i += 1
        return tok

    def parse(self) -> Program:
        stmts = []
        while self.peek():
            stmts.append(self.statement())
        return Program(stmts)

    def statement(self) -> Node:
        tok = self.peek()
        if not tok:
            raise SyntaxError("Unexpected EOF")

        if tok.type == "VAR":
            self.eat("VAR")
            name = self.eat("NAME").value
            self.eat("OP", "=")
            expr = self.expr()
            return VarDecl(name, expr, const=False)

        if tok.type == "CONST":
            self.eat("CONST")
            name = self.eat("NAME").value
            self.eat("OP", "=")
            expr = self.expr()
            return VarDecl(name, expr, const=True)

        if tok.type == "FUNC":
            self.eat("FUNC")
            name = self.eat("NAME").value
            self.eat("SYMBOL", "(")
            params = []
            while self.peek().value != ")":
                params.append(self.eat("NAME").value)
                if self.peek().value == ",":
                    self.eat("SYMBOL", ",")
            self.eat("SYMBOL", ")")
            block = self.block()
            return FuncDef(name, params, block)

        if tok.type == "CLASS":
            self.eat("CLASS")
            name = self.eat("NAME").value
            base = None
            if self.peek().value == ":":
                self.eat("SYMBOL", ":")
                base = self.eat("NAME").value
            block = self.block()
            return ClassDef(name, base, block)

        if tok.type == "NEW":
            self.eat("NEW")
            class_name = self.eat("NAME").value
            self.eat("NAME", "as")
            var_name = self.eat("NAME").value
            return New(class_name, var_name)

        if tok.type == "IF":
            self.eat("IF")
            cond = self.expr()
            then_block = self.block()
            elif_blocks = []
            else_block = None

            while self.peek() and self.peek().type == "ELSE":
                self.eat("ELSE")
                if self.peek().type == "IF":
                    self.eat("IF")
                    cond2 = self.expr()
                    block2 = self.block()
                    elif_blocks.append((cond2, block2))
                else:
                    else_block = self.block()
                    break
            return If(cond, then_block, elif_blocks, else_block)

        if tok.type == "WHILE":
            self.eat("WHILE")
            cond = self.expr()
            block = self.block()
            return While(cond, block)

        if tok.type == "REPEAT":
            self.eat("REPEAT")
            times = self.expr()
            block = self.block()
            return Repeat(times, block)

        if tok.type == "RETURN":
            self.eat("RETURN")
            expr = self.expr() if self.peek() and self.peek().type not in ("SYMBOL",) else None
            return Return(expr)

        if tok.type == "BREAK":
            self.eat("BREAK")
            return Break()

        if tok.type == "CONTINUE":
            self.eat("CONTINUE")
            return Continue()

        if tok.type == "AWAIT":
            self.eat("AWAIT")
            expr = self.expr()
            return Await(expr)

        # assignment or expression
        if tok.type == "NAME" and self.peek(1) and self.peek(1).value == "=":
            name = self.eat("NAME").value
            self.eat("OP", "=")
            expr = self.expr()
            return Assign(Var(name), expr)

        expr = self.expr()
        return ExprStmt(expr)

    def block(self) -> List[Node]:
        self.eat("SYMBOL", "{")
        stmts = []
        while self.peek() and not (self.peek().type == "SYMBOL" and self.peek().value == "}"):
            stmts.append(self.statement())
        self.eat("SYMBOL", "}")
        return stmts

    def expr(self) -> Node:
        return self.logic_or()

    def logic_or(self) -> Node:
        node = self.logic_and()
        while self.peek() and self.peek().type == "OP" and self.peek().value == "OR":
            self.eat("OP", "OR")
            right = self.logic_and()
            node = BinOp(node, "or", right)
        return node

    def logic_and(self) -> Node:
        node = self.logic_not()
        while self.peek() and self.peek().type == "OP" and self.peek().value == "AND":
            self.eat("OP", "AND")
            right = self.logic_not()
            node = BinOp(node, "and", right)
        return node

    def logic_not(self) -> Node:
        if self.peek() and self.peek().type == "OP" and self.peek().value == "NOT":
            self.eat("OP", "NOT")
            return UnaryOp("not", self.logic_not())
        return self.compare()

    def compare(self) -> Node:
        node = self.add_sub()
        while self.peek() and self.peek().type == "OP" and self.peek().value in ("==", "!=", "<", ">", "<=", ">="):
            op = self.eat("OP").value
            right = self.add_sub()
            node = BinOp(node, op, right)
        return node

    def add_sub(self) -> Node:
        node = self.mul_div()
        while self.peek() and self.peek().type == "OP" and self.peek().value in ("+", "-"):
            op = self.eat("OP").value
            right = self.mul_div()
            node = BinOp(node, op, right)
        return node

    def mul_div(self) -> Node:
        node = self.unary()
        while self.peek() and self.peek().type == "OP" and self.peek().value in ("*", "/", "%"):
            op = self.eat("OP").value
            right = self.unary()
            node = BinOp(node, op, right)
        return node

    def unary(self) -> Node:
        if self.peek() and self.peek().type == "OP" and self.peek().value in ("-", "+"):
            op = self.eat("OP").value
            return UnaryOp(op, self.unary())
        return self.primary()

    def primary(self) -> Node:
        tok = self.peek()
        if tok.type == "NUMBER":
            self.eat("NUMBER")
            return Literal(float(tok.value) if "." in tok.value else int(tok.value))
        if tok.type == "STRING":
            self.eat("STRING")
            return Literal(ast.literal_eval(tok.value))
        if tok.type == "TRUE":
            self.eat("TRUE")
            return Literal(True)
        if tok.type == "FALSE":
            self.eat("FALSE")
            return Literal(False)
        if tok.type == "NULL":
            self.eat("NULL")
            return Literal(None)

        if tok.type == "NAME":
            name = self.eat("NAME").value
            node = Var(name)

            # call
            if self.peek() and self.peek().value == "(":
                self.eat("SYMBOL", "(")
                args = []
                while self.peek() and self.peek().value != ")":
                    args.append(self.expr())
                    if self.peek().value == ",":
                        self.eat("SYMBOL", ",")
                self.eat("SYMBOL", ")")
                node = Call(node, args)

            # attribute or index
            while self.peek() and self.peek().value in (".", "["):
                if self.peek().value == ".":
                    self.eat("SYMBOL", ".")
                    attr = self.eat("NAME").value
                    node = Attr(node, attr)
                else:
                    self.eat("SYMBOL", "[")
                    key = self.expr()
                    self.eat("SYMBOL", "]")
                    node = Index(node, key)
            return node

        if tok.value == "[":
            self.eat("SYMBOL", "[")
            items = []
            while self.peek() and self.peek().value != "]":
                items.append(self.expr())
                if self.peek().value == ",":
                    self.eat("SYMBOL", ",")
            self.eat("SYMBOL", "]")
            return Array(items)

        if tok.value == "{":
            self.eat("SYMBOL", "{")
            items = []
            while self.peek() and self.peek().value != "}":
                key = self.expr()
                self.eat("SYMBOL", ":")
                val = self.expr()
                items.append((key, val))
                if self.peek().value == ",":
                    self.eat("SYMBOL", ",")
            self.eat("SYMBOL", "}")
            return DictLit(items)

        if tok.value == "(":
            self.eat("SYMBOL", "(")
            node = self.expr()
            self.eat("SYMBOL", ")")
            return node

        raise SyntaxError(f"Unexpected token: {tok}")

# -------------------------
# Interpreter
# -------------------------
class ReturnSignal(Exception):
    def __init__(self, value):
        self.value = value

class BreakSignal(Exception): pass
class ContinueSignal(Exception): pass

class Emerald:
    def __init__(self):
        self.vars: Dict[str, Any] = {}
        self.consts: set = set()
        self.funcs: Dict[str, FuncDef] = {}
        # class_name -> (base, props, methods)
        self.classes: Dict[str, Tuple[Optional[str], Dict[str, Any], Dict[str, FuncDef]]] = {}

    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.text()

    async def delay(self, ms):
        await asyncio.sleep(ms / 1000)

    async def eval_expr(self, node: Node, local: Dict[str, Any]):
        if isinstance(node, Literal):
            return node.value

        if isinstance(node, Var):
            if node.name == "this":
                return local.get("this")
            if node.name in local:
                return local[node.name]
            return self.vars.get(node.name, None)

        if isinstance(node, Array):
            return [await self.eval_expr(x, local) for x in node.items]

        if isinstance(node, DictLit):
            return {await self.eval_expr(k, local): await self.eval_expr(v, local) for k, v in node.items}

        if isinstance(node, Index):
            obj = await self.eval_expr(node.obj, local)
            key = await self.eval_expr(node.key, local)
            return obj[key]

        if isinstance(node, Attr):
            obj = await self.eval_expr(node.obj, local)
            return obj.get(node.name)

        if isinstance(node, UnaryOp):
            val = await self.eval_expr(node.expr, local)
            if node.op == "-":
                return -val
            if node.op == "+":
                return +val
            if node.op == "not":
                return not val

        if isinstance(node, BinOp):
            l = await self.eval_expr(node.left, local)
            r = await self.eval_expr(node.right, local)
            if node.op == "+":
                return l + r
            if node.op == "-":
                return l - r
            if node.op == "*":
                return l * r
            if node.op == "/":
                return l / r
            if node.op == "%":
                return l % r
            if node.op == "==":
                return l == r
            if node.op == "!=":
                return l != r
            if node.op == "<":
                return l < r
            if node.op == ">":
                return l > r
            if node.op == "<=":
                return l <= r
            if node.op == ">=":
                return l >= r
            if node.op == "and":
                return l and r
            if node.op == "or":
                return l or r

        if isinstance(node, Call):
            return await self.call(node.callee, [await self.eval_expr(a, local) for a in node.args], local)

        if isinstance(node, Await):
            val = await self.eval_expr(node.expr, local)
            return await val

        raise ValueError(f"Unknown expression: {node}")

    async def call(self, callee: Node, args: List[Any], local: Dict[str, Any]):
        # method call
        if isinstance(callee, Attr):
            obj = await self.eval_expr(callee.obj, local)
            method_name = callee.name
            if "__class__" in obj:
                class_name = obj["__class__"]
                method = self.lookup_method(class_name, method_name)
                if not method:
                    raise ValueError(f"Method {method_name} not found on class {class_name}")

                new_local = {"this": obj}
                new_local.update(dict(zip(method.params, args)))
                try:
                    await self.exec_block(method.block, new_local)
                except ReturnSignal as r:
                    return r.value
                return None

        # built-ins
        if isinstance(callee, Var):
            name = callee.name
            if name == "print":
                print(*args)
                return None
            if name == "fetch":
                return await self.fetch(args[0])
            if name == "delay":
                return await self.delay(args[0])

            # user function
            if name not in self.funcs:
                raise ValueError(f"Function {name} not defined")

            func = self.funcs[name]
            if len(args) != len(func.params):
                raise ValueError(f"{name} expected {len(func.params)} args but got {len(args)}")

            new_local = dict(zip(func.params, args))
            try:
                await self.exec_block(func.block, new_local)
            except ReturnSignal as r:
                return r.value
            return None

        raise ValueError("Invalid call target")

    def lookup_method(self, class_name: str, method_name: str):
        # search class chain for method
        if class_name not in self.classes:
            return None
        base, props, methods = self.classes[class_name]
        if method_name in methods:
            return methods[method_name]
        if base:
            return self.lookup_method(base, method_name)
        return None

    async def exec_stmt(self, stmt: Node, local: Dict[str, Any]):
        if isinstance(stmt, VarDecl):
            val = await self.eval_expr(stmt.expr, local)
            if stmt.const:
                self.consts.add(stmt.name)
            self.vars[stmt.name] = val

        elif isinstance(stmt, Assign):
            target = stmt.target
            val = await self.eval_expr(stmt.expr, local)

            if isinstance(target, Var):
                if target.name in self.consts:
                    raise ValueError(f"Cannot assign to const {target.name}")
                if target.name in local:
                    local[target.name] = val
                else:
                    self.vars[target.name] = val

            elif isinstance(target, Attr):
                obj = await self.eval_expr(target.obj, local)
                obj[target.name] = val

            elif isinstance(target, Index):
                obj = await self.eval_expr(target.obj, local)
                key = await self.eval_expr(target.key, local)
                obj[key] = val

            else:
                raise ValueError("Invalid assignment target")

        elif isinstance(stmt, ExprStmt):
            await self.eval_expr(stmt.expr, local)

        elif isinstance(stmt, If):
            if await self.eval_expr(stmt.cond, local):
                await self.exec_block(stmt.then_block, local)
            else:
                done = False
                for cond, block in stmt.elif_blocks:
                    if await self.eval_expr(cond, local):
                        await self.exec_block(block, local)
                        done = True
                        break
                if not done and stmt.else_block:
                    await self.exec_block(stmt.else_block, local)

        elif isinstance(stmt, While):
            while await self.eval_expr(stmt.cond, local):
                try:
                    await self.exec_block(stmt.block, local)
                except BreakSignal:
                    break
                except ContinueSignal:
                    continue

        elif isinstance(stmt, Repeat):
            times = int(await self.eval_expr(stmt.times, local))
            for _ in range(times):
                try:
                    await self.exec_block(stmt.block, local)
                except BreakSignal:
                    break
                except ContinueSignal:
                    continue

        elif isinstance(stmt, FuncDef):
            self.funcs[stmt.name] = stmt

        elif isinstance(stmt, ClassDef):
            props = {}
            methods = {}
            for inner in stmt.props:
                if isinstance(inner, VarDecl):
                    props[inner.name] = inner.expr
                elif isinstance(inner, FuncDef):
                    methods[inner.name] = inner
            self.classes[stmt.name] = (stmt.base, props, methods)

        elif isinstance(stmt, New):
            base, props, methods = self.classes.get(stmt.class_name, (None, {}, {}))
            inst = {"__class__": stmt.class_name}
            if base:
                inst.update(self.new_instance(base))
            # evaluate props
            for k, v in props.items():
                inst[k] = await self.eval_expr(v, local)
            self.vars[stmt.var_name] = inst

        elif isinstance(stmt, Return):
            val = await self.eval_expr(stmt.expr, local) if stmt.expr else None
            raise ReturnSignal(val)

        elif isinstance(stmt, Break):
            raise BreakSignal()

        elif isinstance(stmt, Continue):
            raise ContinueSignal()

        else:
            raise ValueError(f"Unknown statement: {stmt}")

    def new_instance(self, class_name: str):
        if class_name not in self.classes:
            raise ValueError(f"Class {class_name} not defined")
        base, props, methods = self.classes[class_name]
        inst = {"__class__": class_name}
        if base:
            inst.update(self.new_instance(base))
        return inst

    async def exec_block(self, block: List[Node], local: Dict[str, Any]):
        for stmt in block:
            await self.exec_stmt(stmt, local)

    async def run(self, code: str):
        tokens = tokenize(code)
        parser = Parser(tokens)
        prog = parser.parse()
        await self.exec_block(prog.statements, {})

# -------------------------
# REPL
# -------------------------
async def repl():
    interpreter = Emerald()
    while True:
        try:
            code = input("emerald> ")
            if code.strip() == "exit":
                break
            await interpreter.run(code)
        except Exception as e:
            print("ERROR:", e)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        with open(filename, "r") as f:
            code = f.read()
        interpreter = Emerald()
        asyncio.run(interpreter.run(code))
    else:
        asyncio.run(repl())

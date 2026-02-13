namespace mycoolapp;

internal static class EmeraldVm
{
    public static CommandResult Run(CliOptions options)
    {
        try
        {
            ProgramModel prog;
            if (options.Path.EndsWith(".emec", StringComparison.OrdinalIgnoreCase))
            {
                prog = EmeraldCompiler.ReadProgram(options.Path);
            }
            else
            {
                prog = EmeraldCompiler.CompileFile(options.Path);
            }

            var vm = new VM();
            _ = vm.Exec(prog, prog.Main);
            return CommandResult.Ok();
        }
        catch (Exception ex)
        {
            return CommandResult.Fail($"Runtime error: {ex.Message}");
        }
    }
}

internal sealed class VM
{
    private readonly Dictionary<string, Value> _globals = [];

    public Value Exec(ProgramModel prog, List<Instr> code)
    {
        return ExecWithLocals(prog, code, null, "<main>");
    }

    private Value ExecWithLocals(ProgramModel prog, List<Instr> code, List<Value>? locals, string fnName)
    {
        var stack = new List<Value>();
        var pc = 0;

        Value Fail(Exception err, Instr ins) => throw WrapRuntime(err, fnName, ins.Line);

        while (pc < code.Count)
        {
            var ins = code[pc];
            switch (ins.Op)
            {
                case OpCode.PushInt:
                    stack.Add(Value.Int(ins.Int));
                    break;
                case OpCode.PushStr:
                    stack.Add(Value.Str(ins.Str));
                    break;
                case OpCode.PushBool:
                    stack.Add(Value.Bool(ins.Bool));
                    break;
                case OpCode.PushNull:
                    stack.Add(Value.Null());
                    break;
                case OpCode.Load:
                    stack.Add(_globals.TryGetValue(ins.Str, out var v) ? v : Value.Null());
                    break;
                case OpCode.LoadLocal:
                    if (locals is null || ins.Int < 0 || ins.Int >= locals.Count)
                    {
                        return Fail(new Exception($"invalid local slot: {ins.Int}"), ins);
                    }

                    stack.Add(locals[ins.Int]);
                    break;
                case OpCode.Store:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    _globals[ins.Str] = Pop(stack);
                    break;
                case OpCode.StoreLocal:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    if (locals is null || ins.Int < 0 || ins.Int >= locals.Count)
                    {
                        return Fail(new Exception($"invalid local slot: {ins.Int}"), ins);
                    }

                    locals[ins.Int] = Pop(stack);
                    break;
                case OpCode.Print:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    Console.WriteLine(Pop(stack).ToValueString());
                    break;
                case OpCode.Input:
                    Console.Write(ins.Str);
                    stack.Add(Value.Str((Console.ReadLine() ?? "").TrimEnd('\r', '\n')));
                    break;
                case OpCode.Wait:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    Thread.Sleep(Pop(stack).AsInt());
                    break;
                case OpCode.Check:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    var chk = Pop(stack);
                    if (!chk.Truthy())
                    {
                        return Fail(new Exception($"check failed: {ins.Str}"), ins);
                    }

                    break;
                case OpCode.Jmp:
                    pc = ins.Int;
                    continue;
                case OpCode.JmpIfFalse:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    if (!Pop(stack).Truthy())
                    {
                        pc = ins.Int;
                        continue;
                    }

                    break;
                case OpCode.Call:
                    {
                        List<Value> args;
                        try
                        {
                            args = PopArgs(stack, ins.Int);
                        }
                        catch (Exception ex)
                        {
                            return Fail(ex, ins);
                        }

                        if (TryCallBuiltin(ins.Str, args, out var builtin))
                        {
                            stack.Add(builtin);
                            break;
                        }

                        if (!prog.Functions.TryGetValue(ins.Str, out var fn))
                        {
                            return Fail(new Exception($"unknown function: {ins.Str}"), ins);
                        }

                        var size = Math.Max(fn.LocalCount, fn.Params.Count);
                        var localsFrame = Enumerable.Repeat(Value.Null(), size).ToList();
                        for (var i = 0; i < fn.Params.Count && i < args.Count; i++)
                        {
                            localsFrame[i] = args[i];
                        }

                        Value ret;
                        try
                        {
                            ret = ExecWithLocals(prog, fn.Code, localsFrame, ins.Str);
                        }
                        catch (Exception ex)
                        {
                            throw WrapRuntime(ex, fnName, ins.Line);
                        }

                        stack.Add(ret);
                        break;
                    }
                case OpCode.Ret:
                    if (stack.Count == 0)
                    {
                        return Value.Null();
                    }

                    return stack[^1];
                case OpCode.Pop:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    _ = Pop(stack);
                    break;
                case OpCode.Add:
                    TryBinOp(stack, OpAdd, ins, Fail);
                    break;
                case OpCode.Sub:
                    TryBinOp(stack, OpSub, ins, Fail);
                    break;
                case OpCode.Mul:
                    TryBinOp(stack, OpMul, ins, Fail);
                    break;
                case OpCode.Div:
                    TryBinOp(stack, OpDiv, ins, Fail);
                    break;
                case OpCode.Eq:
                    TryBinOp(stack, OpEq, ins, Fail);
                    break;
                case OpCode.Neq:
                    TryBinOp(stack, OpNeq, ins, Fail);
                    break;
                case OpCode.Lt:
                    TryBinOp(stack, OpLt, ins, Fail);
                    break;
                case OpCode.Gt:
                    TryBinOp(stack, OpGt, ins, Fail);
                    break;
                case OpCode.And:
                    TryBinOp(stack, OpAnd, ins, Fail);
                    break;
                case OpCode.Or:
                    TryBinOp(stack, OpOr, ins, Fail);
                    break;
                case OpCode.Not:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    stack.Add(Value.Bool(!Pop(stack).Truthy()));
                    break;
                case OpCode.Neg:
                    if (stack.Count == 0)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    var neg = Pop(stack);
                    if (neg.Kind != "int")
                    {
                        return Fail(new Exception("unary '-' expects int"), ins);
                    }

                    stack.Add(Value.Int(-neg.I));
                    break;
                case OpCode.MakeList:
                    if (stack.Count < ins.Int)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    var items = Enumerable.Repeat(Value.Null(), ins.Int).ToList();
                    for (var i = ins.Int - 1; i >= 0; i--)
                    {
                        items[i] = Pop(stack);
                    }

                    stack.Add(Value.List(items));
                    break;
                case OpCode.IndexGet:
                    if (stack.Count < 2)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    try
                    {
                        var idx = Pop(stack);
                        var target = Pop(stack);
                        stack.Add(IndexGet(target, idx));
                    }
                    catch (Exception ex)
                    {
                        return Fail(ex, ins);
                    }

                    break;
                case OpCode.IndexSet:
                    if (stack.Count < 3)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    try
                    {
                        var val = Pop(stack);
                        var idx = Pop(stack);
                        var target = Pop(stack);
                        IndexSet(target, idx, val);
                    }
                    catch (Exception ex)
                    {
                        return Fail(ex, ins);
                    }

                    break;
                case OpCode.Slice:
                    if (stack.Count < 3)
                    {
                        return Fail(new Exception("stack underflow"), ins);
                    }

                    try
                    {
                        var end = Pop(stack);
                        var start = Pop(stack);
                        var target = Pop(stack);
                        stack.Add(DoSlice(target, start, end));
                    }
                    catch (Exception ex)
                    {
                        return Fail(ex, ins);
                    }

                    break;
                case OpCode.Noop:
                    break;
                default:
                    return Fail(new Exception("unknown opcode"), ins);
            }

            pc++;
        }

        return Value.Null();
    }

    private static RuntimeError WrapRuntime(Exception err, string fnName, int line)
    {
        if (err is RuntimeError rt)
        {
            rt.Frames.Add(new TraceFrame { Function = fnName, Line = line });
            return rt;
        }

        return new RuntimeError(err.Message, [new TraceFrame { Function = fnName, Line = line }]);
    }

    private static Value Pop(List<Value> stack)
    {
        var v = stack[^1];
        stack.RemoveAt(stack.Count - 1);
        return v;
    }

    private static List<Value> PopArgs(List<Value> stack, int n)
    {
        if (stack.Count < n)
        {
            throw new Exception("stack underflow");
        }

        var args = Enumerable.Repeat(Value.Null(), n).ToList();
        for (var i = n - 1; i >= 0; i--)
        {
            args[i] = Pop(stack);
        }

        return args;
    }

    private static void TryBinOp(List<Value> stack, Func<Value, Value, Value> op, Instr ins, Func<Exception, Instr, Value> fail)
    {
        try
        {
            if (stack.Count < 2)
            {
                throw new Exception("stack underflow");
            }

            var r = Pop(stack);
            var l = Pop(stack);
            stack.Add(op(l, r));
        }
        catch (Exception ex)
        {
            _ = fail(ex, ins);
        }
    }

    private bool TryCallBuiltin(string name, List<Value> args, out Value result)
    {
        switch (name)
        {
            case "table":
            case "dict":
                if (args.Count % 2 != 0)
                {
                    throw new Exception("dict expects key/value pairs");
                }

                var map = new Dictionary<string, Value>();
                for (var i = 0; i < args.Count; i += 2)
                {
                    map[args[i].ToValueString()] = args[i + 1];
                }

                result = Value.Dict(map);
                return true;
            case "len":
                if (args.Count != 1)
                {
                    throw new Exception("len expects 1 argument");
                }

                result = args[0].Kind switch
                {
                    "string" => Value.Int(args[0].S.Length),
                    "list" => Value.Int(args[0].L.Count),
                    "dict" => Value.Int(args[0].M.Count),
                    _ => throw new Exception("len expects string, list, or dict"),
                };
                return true;
            case "append":
                if (args.Count != 2)
                {
                    throw new Exception("append expects 2 arguments");
                }

                if (args[0].Kind != "list")
                {
                    throw new Exception("append expects list as first argument");
                }

                var outList = new List<Value>(args[0].L) { args[1] };
                result = Value.List(outList);
                return true;
            case "keys":
                if (args.Count != 1)
                {
                    throw new Exception("keys expects 1 argument");
                }

                if (args[0].Kind != "dict")
                {
                    throw new Exception("keys expects dict");
                }

                result = Value.List(args[0].M.Keys.Select(Value.Str).ToList());
                return true;
            case "input":
            case "read":
                if (args.Count > 1)
                {
                    throw new Exception("input expects 0 or 1 argument");
                }

                var prompt = args.Count == 1 ? args[0].ToValueString() : "";
                Console.Write(prompt);
                result = Value.Str((Console.ReadLine() ?? "").TrimEnd('\r', '\n'));
                return true;
            case "plc":
                if (args.Count != 1)
                {
                    throw new Exception("plc expects 1 argument");
                }

                result = Value.Str(args[0].ToValueString());
                return true;
            case "math_abs":
                if (args.Count != 1)
                {
                    throw new Exception("math_abs expects 1 argument");
                }

                result = Value.Int(Math.Abs(args[0].AsInt()));
                return true;
            case "math_min":
                if (args.Count != 2)
                {
                    throw new Exception("math_min expects 2 arguments");
                }

                result = Value.Int(Math.Min(args[0].AsInt(), args[1].AsInt()));
                return true;
            case "math_max":
                if (args.Count != 2)
                {
                    throw new Exception("math_max expects 2 arguments");
                }

                result = Value.Int(Math.Max(args[0].AsInt(), args[1].AsInt()));
                return true;
            case "math_pow":
                if (args.Count != 2)
                {
                    throw new Exception("math_pow expects 2 arguments");
                }

                result = Value.Int((int)Math.Pow(args[0].AsInt(), args[1].AsInt()));
                return true;
            default:
                result = Value.Null();
                return false;
        }
    }

    private static Value IndexGet(Value target, Value idx)
    {
        return target.Kind switch
        {
            "list" => target.L[ResolveIndex(idx, target.L.Count)],
            "dict" => target.M.TryGetValue(idx.ToValueString(), out var v) ? v : Value.Null(),
            "string" => Value.Str(target.S[ResolveIndex(idx, target.S.Length)].ToString()),
            _ => throw new Exception("indexing expects list, dict, or string"),
        };
    }

    private static void IndexSet(Value target, Value idx, Value val)
    {
        switch (target.Kind)
        {
            case "list":
                target.L[ResolveIndex(idx, target.L.Count)] = val;
                return;
            case "dict":
                target.M[idx.ToValueString()] = val;
                return;
            default:
                throw new Exception("index assignment expects list or dict");
        }
    }

    private static Value DoSlice(Value target, Value start, Value end)
    {
        switch (target.Kind)
        {
            case "list":
                {
                    var (s, e) = ResolveSliceBounds(target.L.Count, start, end);
                    return Value.List(target.L.GetRange(s, e - s));
                }
            case "string":
                {
                    var (s, e) = ResolveSliceBounds(target.S.Length, start, end);
                    return Value.Str(target.S[s..e]);
                }
            default:
                throw new Exception("slice expects list or string");
        }
    }

    private static int ResolveIndex(Value v, int length)
    {
        if (v.Kind != "int")
        {
            throw new Exception("index must be int");
        }

        var i = v.I;
        if (i < 0)
        {
            i = length + i;
        }

        if (i < 0 || i >= length)
        {
            throw new Exception($"index out of range: {v.I}");
        }

        return i;
    }

    private static (int S, int E) ResolveSliceBounds(int length, Value start, Value end)
    {
        var s = 0;
        var e = length;
        if (start.Kind != "null")
        {
            if (start.Kind != "int")
            {
                throw new Exception("slice start must be int");
            }

            s = start.I;
            if (s < 0)
            {
                s = length + s;
            }
        }

        if (end.Kind != "null")
        {
            if (end.Kind != "int")
            {
                throw new Exception("slice end must be int");
            }

            e = end.I;
            if (e < 0)
            {
                e = length + e;
            }
        }

        if (s < 0)
        {
            s = 0;
        }

        if (e > length)
        {
            e = length;
        }

        if (s > e)
        {
            s = e;
        }

        return (s, e);
    }

    private static Value OpAdd(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            return Value.Int(a.I + b.I);
        }

        if (a.Kind == "string" || b.Kind == "string")
        {
            return Value.Str(a.ToValueString() + b.ToValueString());
        }

        if (a.Kind == "list" && b.Kind == "list")
        {
            var outList = new List<Value>(a.L);
            outList.AddRange(b.L);
            return Value.List(outList);
        }

        throw new Exception("'+' expects int, string, or list");
    }

    private static Value OpSub(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            return Value.Int(a.I - b.I);
        }

        throw new Exception("'-' expects int");
    }

    private static Value OpMul(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            return Value.Int(a.I * b.I);
        }

        throw new Exception("'*' expects int");
    }

    private static Value OpDiv(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            if (b.I == 0)
            {
                throw new Exception("division by zero");
            }

            return Value.Int(a.I / b.I);
        }

        throw new Exception("'/' expects int");
    }

    private static Value OpEq(Value a, Value b) => Value.Bool(a.Equal(b));

    private static Value OpNeq(Value a, Value b) => Value.Bool(!a.Equal(b));

    private static Value OpLt(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            return Value.Bool(a.I < b.I);
        }

        if (a.Kind == "string" && b.Kind == "string")
        {
            return Value.Bool(string.CompareOrdinal(a.S, b.S) < 0);
        }

        throw new Exception("'<' expects int or string");
    }

    private static Value OpGt(Value a, Value b)
    {
        if (a.Kind == "int" && b.Kind == "int")
        {
            return Value.Bool(a.I > b.I);
        }

        if (a.Kind == "string" && b.Kind == "string")
        {
            return Value.Bool(string.CompareOrdinal(a.S, b.S) > 0);
        }

        throw new Exception("'>' expects int or string");
    }

    private static Value OpAnd(Value a, Value b) => Value.Bool(a.Truthy() && b.Truthy());

    private static Value OpOr(Value a, Value b) => Value.Bool(a.Truthy() || b.Truthy());
}

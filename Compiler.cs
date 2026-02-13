using System.Text;
using System.Text.Json;

namespace mycoolapp;

internal static class EmeraldCompiler
{
    public static CommandResult Build(CliOptions options)
    {
        try
        {
            var prog = CompileFile(options.Path);
            var outPath = Path.ChangeExtension(options.Path, ".emec")!;
            WriteProgram(outPath, prog);
            return CommandResult.Ok($"Wrote {outPath}");
        }
        catch (Exception ex)
        {
            return CommandResult.Fail($"Compile error: {ex.Message}");
        }
    }

    public static ProgramModel CompileFile(string path)
    {
        var data = File.ReadAllText(path);
        var tokens = EmeraldParser.Tokenize(data);
        var c = new Compiler(tokens);
        c.PredeclareFunctions();
        var mainCode = new List<Instr>();
        var globalTypes = new TypeEnv(null);
        c.CompileBlock(mainCode, [], globalTypes, ValueType.Unknown, null);
        return new ProgramModel { Version = 2, Main = mainCode, Functions = c.Functions };
    }

    public static void WriteProgram(string path, ProgramModel prog)
    {
        var json = JsonSerializer.Serialize(prog);
        File.WriteAllText(path, json);
    }

    public static ProgramModel ReadProgram(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<ProgramModel>(json) ?? throw new Exception("invalid .emec file");
    }
}

internal sealed class Compiler
{
    private readonly List<Token> _tokens;
    private int _idx;

    public Dictionary<string, FunctionDef> Functions { get; } = [];

    public Compiler(List<Token> tokens)
    {
        _tokens = tokens;
    }

    private Token? Next()
    {
        if (_idx >= _tokens.Count)
        {
            return null;
        }

        return _tokens[_idx++];
    }

    private Token? Peek()
    {
        if (_idx >= _tokens.Count)
        {
            return null;
        }

        return _tokens[_idx];
    }

    public void PredeclareFunctions()
    {
        foreach (var t in _tokens)
        {
            if (t.Kind != TokenKind.Line || !t.Text.StartsWith("fnc "))
            {
                continue;
            }

            var (name, pars, paramTypes, retType) = ParseFnDecl(t.Text);
            if (Functions.ContainsKey(name))
            {
                throw new Exception($"line {t.Line}: duplicate function name: {name}");
            }

            Functions[name] = new FunctionDef { Params = pars, ParamTypes = paramTypes, ReturnType = retType };
        }
    }

    public void CompileBlock(List<Instr> code, List<LoopCtx> loopStack, TypeEnv? types, ValueType retType, CompileCtx? ctx)
    {
        while (true)
        {
            var t = Next();
            if (t is null)
            {
                return;
            }

            if (t.Kind == TokenKind.RBrace)
            {
                return;
            }

            if (t.Kind == TokenKind.LBrace)
            {
                throw new Exception($"line {t.Line}: unexpected '{{'");
            }

            var line = t.Text;
            if (line == "" || line.StartsWith("#") || line.StartsWith("--"))
            {
                continue;
            }

            if (line.StartsWith("fnc "))
            {
                CompileFunctionDecl(line, t.Line, types);
                continue;
            }

            if (line.StartsWith("var"))
            {
                CompileVar(line, code, types, ctx, t.Line);
                continue;
            }

            if (line.StartsWith("print(") && line.EndsWith(")"))
            {
                var start = code.Count;
                var inner = line[6..^1].Trim();
                CompileExpr(inner, code, types, ctx, t.Line);
                code.Add(new Instr { Op = OpCode.Print, Line = t.Line });
                SetInstrLine(code, start, t.Line);
                continue;
            }

            if (line.StartsWith("input(plc(") && line.EndsWith("))"))
            {
                var start = code.Count;
                var inner = line[10..^2].Trim();
                var (s, ok) = EmeraldParser.ParseStringLiteral(inner);
                if (!ok)
                {
                    throw new Exception($"line {t.Line}: input requires a string literal prompt");
                }

                code.Add(new Instr { Op = OpCode.PushStr, Str = s, Line = t.Line });
                code.Add(new Instr { Op = OpCode.Call, Str = "input", Int = 1, Line = t.Line });
                code.Add(new Instr { Op = OpCode.Pop, Line = t.Line });
                SetInstrLine(code, start, t.Line);
                continue;
            }

            if (line.StartsWith("wait(") && line.EndsWith(")"))
            {
                var start = code.Count;
                var inner = line[5..^1].Trim();
                var exprNode = EmeraldParser.ParseExprAst(inner);
                if (types is not null)
                {
                    var et = InferExprType(exprNode, types);
                    if (et != ValueType.Unknown && et != ValueType.Int)
                    {
                        throw new Exception($"line {t.Line}: wait expects int milliseconds");
                    }
                }

                CompileExprNode(exprNode, code, types, ctx, t.Line);
                code.Add(new Instr { Op = OpCode.Wait, Line = t.Line });
                SetInstrLine(code, start, t.Line);
                continue;
            }

            if (line.StartsWith("check "))
            {
                var start = code.Count;
                var inner = line[6..].Trim();
                CompileExpr(inner, code, types, ctx, t.Line);
                code.Add(new Instr { Op = OpCode.Check, Str = inner, Line = t.Line });
                SetInstrLine(code, start, t.Line);
                continue;
            }

            if (line == "brk")
            {
                if (loopStack.Count == 0)
                {
                    throw new Exception($"line {t.Line}: brk used outside loop");
                }

                var idx = code.Count;
                code.Add(new Instr { Op = OpCode.Jmp, Int = -1, Line = t.Line });
                loopStack[^1].BreakJumps.Add(idx);
                continue;
            }

            if (line == "cont")
            {
                if (loopStack.Count == 0)
                {
                    throw new Exception($"line {t.Line}: cont used outside loop");
                }

                var idx = code.Count;
                code.Add(new Instr { Op = OpCode.Jmp, Int = -1, Line = t.Line });
                loopStack[^1].ContinueJumps.Add(idx);
                continue;
            }

            if (line == "return" || line.StartsWith("return "))
            {
                CompileReturn(line, code, types, ctx, retType, t.Line);
                continue;
            }

            if (line.StartsWith("if(") && line.EndsWith(")"))
            {
                CompileIf(line, code, loopStack, types, ctx, retType, t.Line);
                continue;
            }

            if (line.StartsWith("for(") && line.EndsWith(")"))
            {
                CompileFor(line, code, loopStack, types, ctx, retType, t.Line);
                continue;
            }

            if (line.StartsWith("while(") && line.EndsWith(")"))
            {
                CompileWhile(line, code, loopStack, types, ctx, retType, t.Line);
                continue;
            }

            var (target, expr, isAssign) = SplitAssignment(line);
            if (isAssign)
            {
                CompileAssignment(target, expr, code, types, ctx, t.Line);
                continue;
            }

            var exprStart = code.Count;
            try
            {
                CompileExpr(line, code, types, ctx, t.Line);
                code.Add(new Instr { Op = OpCode.Pop, Line = t.Line });
                SetInstrLine(code, exprStart, t.Line);
                continue;
            }
            catch
            {
                code.RemoveRange(exprStart, code.Count - exprStart);
            }

            throw new Exception($"line {t.Line}: unknown statement: {line}");
        }
    }

    private void CompileFunctionDecl(string line, int lineNo, TypeEnv? parentTypes)
    {
        var (name, parameters, paramTypes, fnRet) = ParseFnDecl(line);
        var p = Next();
        if (p is null || p.Kind != TokenKind.LBrace)
        {
            throw new Exception($"line {lineNo}: fnc must be followed by '{{'");
        }

        var fnCode = new List<Instr>();
        var fnTypes = new TypeEnv(parentTypes);
        var fnCtx = new CompileCtx { InFunction = true, Scope = new LocalScope(null) };
        for (var i = 0; i < parameters.Count; i++)
        {
            var slot = fnCtx.DefineLocal(parameters[i]);
            if (slot != i)
            {
                throw new Exception($"line {lineNo}: internal error: parameter slot mismatch");
            }

            fnTypes.SetLocal(parameters[i], i < paramTypes.Count ? paramTypes[i] : ValueType.Unknown);
        }

        CompileBlock(fnCode, [], fnTypes, fnRet, fnCtx);
        Functions[name] = new FunctionDef
        {
            Params = parameters,
            ParamTypes = paramTypes,
            ReturnType = fnRet,
            LocalCount = fnCtx.NextSlot,
            Code = fnCode,
        };
    }

    private void CompileReturn(string line, List<Instr> code, TypeEnv? types, CompileCtx? ctx, ValueType retType, int lineNo)
    {
        var start = code.Count;
        var expr = line[6..].Trim();
        if (expr != "")
        {
            var exprNode = EmeraldParser.ParseExprAst(expr);
            if (types is not null)
            {
                var exprType = InferExprType(exprNode, types);
                if (retType != ValueType.Unknown && exprType == ValueType.Unknown)
                {
                    throw new Exception($"line {lineNo}: type mismatch: return is {retType.ToTypeString()}, got {exprType.ToTypeString()}");
                }

                if (retType != ValueType.Unknown && !Assignable(retType, exprType))
                {
                    throw new Exception($"line {lineNo}: type mismatch: return is {retType.ToTypeString()}, got {exprType.ToTypeString()}");
                }
            }

            CompileExprNode(exprNode, code, types, ctx, lineNo);
            code.Add(new Instr { Op = OpCode.Ret, Line = lineNo });
        }
        else
        {
            if (retType != ValueType.Unknown && !Assignable(retType, ValueType.Null))
            {
                throw new Exception($"line {lineNo}: type mismatch: return is {retType.ToTypeString()}, got {ValueType.Null.ToTypeString()}");
            }

            code.Add(new Instr { Op = OpCode.PushNull, Line = lineNo });
            code.Add(new Instr { Op = OpCode.Ret, Line = lineNo });
        }

        SetInstrLine(code, start, lineNo);
    }
    private void CompileIf(string line, List<Instr> code, List<LoopCtx> loopStack, TypeEnv? types, CompileCtx? ctx, ValueType retType, int lineNo)
    {
        var cond = line[3..^1].Trim();
        var start = code.Count;
        CompileExpr(cond, code, types, ctx, lineNo);
        var jmpIfFalseIdx = code.Count;
        code.Add(new Instr { Op = OpCode.JmpIfFalse, Int = -1, Line = lineNo });
        var p = Next();
        if (p is null || p.Kind != TokenKind.LBrace)
        {
            throw new Exception($"line {lineNo}: if must be followed by '{{'");
        }

        var ifCtx = ctx?.ChildScope();
        CompileBlock(code, loopStack, new TypeEnv(types), retType, ifCtx);
        if (ifCtx is not null && ctx is not null && ifCtx.NextSlot > ctx.NextSlot)
        {
            ctx.NextSlot = ifCtx.NextSlot;
        }

        var endJumps = new List<int> { code.Count };
        code.Add(new Instr { Op = OpCode.Jmp, Int = -1, Line = lineNo });
        SetInstrLine(code, start, lineNo);

        while (true)
        {
            var n = Peek();
            if (n is null || n.Kind != TokenKind.Line)
            {
                break;
            }

            var nextLine = n.Text.Trim();
            if (nextLine.StartsWith("else if(") && nextLine.EndsWith(")"))
            {
                _ = Next();
                code[jmpIfFalseIdx].Int = code.Count;
                var cond2 = nextLine[8..^1].Trim();
                var start2 = code.Count;
                CompileExpr(cond2, code, types, ctx, n.Line);
                jmpIfFalseIdx = code.Count;
                code.Add(new Instr { Op = OpCode.JmpIfFalse, Int = -1, Line = n.Line });
                p = Next();
                if (p is null || p.Kind != TokenKind.LBrace)
                {
                    throw new Exception($"line {n.Line}: else if must be followed by '{{'");
                }

                var elseIfCtx = ctx?.ChildScope();
                CompileBlock(code, loopStack, new TypeEnv(types), retType, elseIfCtx);
                if (elseIfCtx is not null && ctx is not null && elseIfCtx.NextSlot > ctx.NextSlot)
                {
                    ctx.NextSlot = elseIfCtx.NextSlot;
                }

                endJumps.Add(code.Count);
                code.Add(new Instr { Op = OpCode.Jmp, Int = -1, Line = n.Line });
                SetInstrLine(code, start2, n.Line);
                continue;
            }

            if (nextLine == "else")
            {
                _ = Next();
                code[jmpIfFalseIdx].Int = code.Count;
                p = Next();
                if (p is null || p.Kind != TokenKind.LBrace)
                {
                    throw new Exception($"line {n.Line}: else must be followed by '{{'");
                }

                var elseCtx = ctx?.ChildScope();
                CompileBlock(code, loopStack, new TypeEnv(types), retType, elseCtx);
                if (elseCtx is not null && ctx is not null && elseCtx.NextSlot > ctx.NextSlot)
                {
                    ctx.NextSlot = elseCtx.NextSlot;
                }

                jmpIfFalseIdx = -1;
            }

            break;
        }

        if (jmpIfFalseIdx != -1)
        {
            code[jmpIfFalseIdx].Int = code.Count;
        }

        var end = code.Count;
        foreach (var idx in endJumps)
        {
            code[idx].Int = end;
        }
    }

    private void CompileFor(string line, List<Instr> code, List<LoopCtx> loopStack, TypeEnv? types, CompileCtx? ctx, ValueType retType, int lineNo)
    {
        var header = line[4..^1].Trim();
        var forIn = ParseForInHeader(header);
        if (forIn.Ok)
        {
            CompileForIn(forIn.Name, forIn.Expr, code, loopStack, types, ctx, retType, lineNo);
            return;
        }

        var p = Next();
        if (p is null || p.Kind != TokenKind.LBrace)
        {
            throw new Exception($"line {lineNo}: for must be followed by '{{'");
        }

        var loopStart = code.Count;
        var hasCond = !header.Equals("true", StringComparison.OrdinalIgnoreCase);
        var jmpIfFalseIdx = -1;
        var start = code.Count;
        if (hasCond)
        {
            CompileExpr(header, code, types, ctx, lineNo);
            jmpIfFalseIdx = code.Count;
            code.Add(new Instr { Op = OpCode.JmpIfFalse, Int = -1, Line = lineNo });
        }

        var loop = new LoopCtx { Start = loopStart };
        loopStack.Add(loop);
        var loopCtxState = ctx?.ChildScope();
        CompileBlock(code, loopStack, new TypeEnv(types), retType, loopCtxState);
        code.Add(new Instr { Op = OpCode.Jmp, Int = loopStart, Line = lineNo });
        var loopEnd = code.Count;
        if (hasCond)
        {
            code[jmpIfFalseIdx].Int = loopEnd;
        }

        var cur = loopStack[^1];
        foreach (var bi in cur.BreakJumps)
        {
            code[bi].Int = loopEnd;
        }

        foreach (var ci in cur.ContinueJumps)
        {
            code[ci].Int = loopStart;
        }

        loopStack.RemoveAt(loopStack.Count - 1);
        if (loopCtxState is not null && ctx is not null && loopCtxState.NextSlot > ctx.NextSlot)
        {
            ctx.NextSlot = loopCtxState.NextSlot;
        }

        SetInstrLine(code, start, lineNo);
    }

    private void CompileForIn(string itName, string itExpr, List<Instr> code, List<LoopCtx> loopStack, TypeEnv? types, CompileCtx? ctx, ValueType retType, int lineNo)
    {
        var p = Next();
        if (p is null || p.Kind != TokenKind.LBrace)
        {
            throw new Exception($"line {lineNo}: for must be followed by '{{'");
        }

        var start = code.Count;
        var iterList = $"__iter_list_{start}";
        var iterIdx = $"__iter_idx_{start}";

        CompileExpr(itExpr, code, types, ctx, lineNo);
        EmitStore(iterList, code, ctx, lineNo);
        code.Add(new Instr { Op = OpCode.PushInt, Int = 0, Line = lineNo });
        EmitStore(iterIdx, code, ctx, lineNo);

        var loopStart = code.Count;
        EmitLoad(iterIdx, code, ctx, lineNo);
        EmitLoad(iterList, code, ctx, lineNo);
        code.Add(new Instr { Op = OpCode.Call, Str = "len", Int = 1, Line = lineNo });
        code.Add(new Instr { Op = OpCode.Lt, Line = lineNo });
        var jmpIfFalseIdx = code.Count;
        code.Add(new Instr { Op = OpCode.JmpIfFalse, Int = -1, Line = lineNo });

        EmitLoad(iterList, code, ctx, lineNo);
        EmitLoad(iterIdx, code, ctx, lineNo);
        code.Add(new Instr { Op = OpCode.IndexGet, Line = lineNo });
        var bodyCtx = ctx?.ChildScope();
        EmitStore(itName, code, bodyCtx, lineNo);
        var bodyTypes = new TypeEnv(types);
        bodyTypes.SetLocal(itName, ValueType.Unknown);
        var loop = new LoopCtx { Start = -1 };
        loopStack.Add(loop);
        CompileBlock(code, loopStack, bodyTypes, retType, bodyCtx);

        var stepStart = code.Count;
        EmitLoad(iterIdx, code, ctx, lineNo);
        code.Add(new Instr { Op = OpCode.PushInt, Int = 1, Line = lineNo });
        code.Add(new Instr { Op = OpCode.Add, Line = lineNo });
        EmitStore(iterIdx, code, ctx, lineNo);
        code.Add(new Instr { Op = OpCode.Jmp, Int = loopStart, Line = lineNo });
        var loopEnd = code.Count;
        code[jmpIfFalseIdx].Int = loopEnd;
        var cur = loopStack[^1];
        foreach (var bi in cur.BreakJumps)
        {
            code[bi].Int = loopEnd;
        }

        foreach (var ci in cur.ContinueJumps)
        {
            code[ci].Int = stepStart;
        }

        loopStack.RemoveAt(loopStack.Count - 1);
        if (bodyCtx is not null && ctx is not null && bodyCtx.NextSlot > ctx.NextSlot)
        {
            ctx.NextSlot = bodyCtx.NextSlot;
        }

        SetInstrLine(code, start, lineNo);
    }

    private void CompileWhile(string line, List<Instr> code, List<LoopCtx> loopStack, TypeEnv? types, CompileCtx? ctx, ValueType retType, int lineNo)
    {
        var cond = line[6..^1].Trim();
        var p = Next();
        if (p is null || p.Kind != TokenKind.LBrace)
        {
            throw new Exception($"line {lineNo}: while must be followed by '{{'");
        }

        var loopStart = code.Count;
        var start = code.Count;
        CompileExpr(cond, code, types, ctx, lineNo);
        var jmpIfFalseIdx = code.Count;
        code.Add(new Instr { Op = OpCode.JmpIfFalse, Int = -1, Line = lineNo });
        var loop = new LoopCtx { Start = loopStart };
        loopStack.Add(loop);
        var loopCtxState = ctx?.ChildScope();
        CompileBlock(code, loopStack, new TypeEnv(types), retType, loopCtxState);
        code.Add(new Instr { Op = OpCode.Jmp, Int = loopStart, Line = lineNo });
        var loopEnd = code.Count;
        code[jmpIfFalseIdx].Int = loopEnd;
        var cur = loopStack[^1];
        foreach (var bi in cur.BreakJumps)
        {
            code[bi].Int = loopEnd;
        }

        foreach (var ci in cur.ContinueJumps)
        {
            code[ci].Int = loopStart;
        }

        loopStack.RemoveAt(loopStack.Count - 1);
        if (loopCtxState is not null && ctx is not null && loopCtxState.NextSlot > ctx.NextSlot)
        {
            ctx.NextSlot = loopCtxState.NextSlot;
        }

        SetInstrLine(code, start, lineNo);
    }

    private void CompileAssignment(string target, string expr, List<Instr> code, TypeEnv? types, CompileCtx? ctx, int lineNo)
    {
        if (target == "")
        {
            throw new Exception($"line {lineNo}: invalid assignment");
        }

        if (target.Contains('(') || target.Contains(')'))
        {
            throw new Exception($"line {lineNo}: type annotations are only allowed in var declarations");
        }

        var targetNode = EmeraldParser.ParseExprAst(target);
        var exprNode = EmeraldParser.ParseExprAst(expr);

        if (types is not null)
        {
            var exprType = InferExprType(exprNode, types);
            switch (targetNode.Kind)
            {
                case ExprKind.Ident:
                    var name = targetNode.Name;
                    if (types.GetLocal(name, out var existing))
                    {
                        if (existing != ValueType.Unknown && exprType == ValueType.Unknown)
                        {
                            throw new Exception($"line {lineNo}: type mismatch: {name} is {existing.ToTypeString()}, got {exprType.ToTypeString()}");
                        }

                        if (!Assignable(existing, exprType))
                        {
                            throw new Exception($"line {lineNo}: type mismatch: {name} is {existing.ToTypeString()}, got {exprType.ToTypeString()}");
                        }
                    }
                    else
                    {
                        types.SetLocal(name, exprType == ValueType.Unknown ? ValueType.Unknown : exprType);
                    }

                    break;
                case ExprKind.Index:
                    var targetType = InferExprType(targetNode.Left!, types);
                    if (targetType != ValueType.Unknown && targetType != ValueType.List && targetType != ValueType.Dict)
                    {
                        throw new Exception($"line {lineNo}: index assignment expects list or dict target");
                    }

                    break;
                default:
                    throw new Exception($"line {lineNo}: invalid assignment target");
            }
        }

        var start = code.Count;
        switch (targetNode.Kind)
        {
            case ExprKind.Ident:
                CompileExprNode(exprNode, code, types, ctx, lineNo);
                EmitStore(targetNode.Name, code, ctx, lineNo);
                break;
            case ExprKind.Index:
                CompileExprNode(targetNode.Left!, code, types, ctx, lineNo);
                CompileExprNode(targetNode.Right!, code, types, ctx, lineNo);
                CompileExprNode(exprNode, code, types, ctx, lineNo);
                code.Add(new Instr { Op = OpCode.IndexSet, Line = lineNo });
                break;
            default:
                throw new Exception($"line {lineNo}: invalid assignment target");
        }

        SetInstrLine(code, start, lineNo);
    }
    private void CompileVar(string line, List<Instr> code, TypeEnv? types, CompileCtx? ctx, int lineNo)
    {
        var trimmed = line.Trim();
        if (trimmed == "var")
        {
            var p = Next();
            if (p is null || p.Kind != TokenKind.LBrace)
            {
                throw new Exception("var block must be followed by '{'");
            }

            var itemsText = CollectBlockText();
            var specs = EmeraldParser.SplitTopLevel(itemsText, ',');
            foreach (var specRaw in specs)
            {
                var spec = specRaw.Trim();
                if (spec == "")
                {
                    continue;
                }

                CompileVarSpec(spec, code, types, ctx, lineNo);
            }

            return;
        }

        if (trimmed.StartsWith("var "))
        {
            var spec = trimmed[4..].Trim();
            if (spec.StartsWith("{"))
            {
                var itemsText = spec[1..].Trim();
                if (itemsText.EndsWith("}"))
                {
                    itemsText = itemsText[..^1].Trim();
                }
                else
                {
                    itemsText += " " + CollectBlockText();
                }

                var specs = EmeraldParser.SplitTopLevel(itemsText, ',');
                foreach (var sraw in specs)
                {
                    var s = sraw.Trim();
                    if (s == "")
                    {
                        continue;
                    }

                    CompileVarSpec(s, code, types, ctx, lineNo);
                }

                return;
            }

            CompileVarSpec(spec, code, types, ctx, lineNo);
        }
    }

    private string CollectBlockText()
    {
        var b = new StringBuilder();
        var depth = 1;
        while (depth > 0)
        {
            var t = Next();
            if (t is null)
            {
                break;
            }

            if (t.Kind == TokenKind.LBrace)
            {
                depth++;
                continue;
            }

            if (t.Kind == TokenKind.RBrace)
            {
                depth--;
                if (depth == 0)
                {
                    break;
                }

                continue;
            }

            if (t.Kind == TokenKind.Line)
            {
                if (b.Length > 0)
                {
                    b.Append(' ');
                }

                b.Append(t.Text);
            }
        }

        return b.ToString();
    }

    private void CompileVarSpec(string spec, List<Instr> code, TypeEnv? types, CompileCtx? ctx, int lineNo)
    {
        var (name, typ, expr, hasExpr) = ParseVarSpec(spec);
        var declaredType = ValueType.Unknown;
        if (typ != "")
        {
            if (!TryParseTypeName(typ, out declaredType))
            {
                throw new Exception($"unknown type: {typ}");
            }
        }

        if (!hasExpr)
        {
            types?.SetLocal(name, declaredType == ValueType.Unknown ? ValueType.Unknown : declaredType);
            code.Add(new Instr { Op = OpCode.PushNull, Line = lineNo });
            if (ctx is not null && ctx.InFunction && ctx.Scope is not null)
            {
                if (!ctx.Scope.GetLocal(name, out var slot))
                {
                    slot = ctx.DefineLocal(name);
                }

                code.Add(new Instr { Op = OpCode.StoreLocal, Int = slot, Line = lineNo });
            }
            else
            {
                code.Add(new Instr { Op = OpCode.Store, Str = name, Line = lineNo });
            }

            return;
        }

        var exprNode = EmeraldParser.ParseExprAst(expr);
        if (types is not null)
        {
            var exprType = InferExprType(exprNode, types);
            if (declaredType != ValueType.Unknown)
            {
                if (exprType == ValueType.Unknown)
                {
                    throw new Exception($"type mismatch: {name} is {declaredType.ToTypeString()}, got {exprType.ToTypeString()}");
                }

                if (!Assignable(declaredType, exprType))
                {
                    throw new Exception($"type mismatch: {name} is {declaredType.ToTypeString()}, got {exprType.ToTypeString()}");
                }

                types.SetLocal(name, declaredType);
            }
            else
            {
                types.SetLocal(name, exprType == ValueType.Unknown ? ValueType.Unknown : exprType);
            }
        }

        CompileExprNode(exprNode, code, types, ctx, lineNo);
        if (ctx is not null && ctx.InFunction && ctx.Scope is not null)
        {
            if (!ctx.Scope.GetLocal(name, out var slot))
            {
                slot = ctx.DefineLocal(name);
            }

            code.Add(new Instr { Op = OpCode.StoreLocal, Int = slot, Line = lineNo });
        }
        else
        {
            code.Add(new Instr { Op = OpCode.Store, Str = name, Line = lineNo });
        }
    }

    private static (string Name, string Typ, string Expr, bool HasExpr) ParseVarSpec(string spec)
    {
        spec = spec.Trim();
        if (spec == "")
        {
            throw new Exception("empty var spec");
        }

        var nameEnd = spec.IndexOfAny(['(', '=']);
        if (nameEnd == -1)
        {
            var onlyName = spec.Trim();
            if (!EmeraldParser.IsIdent(onlyName))
            {
                throw new Exception("invalid var name");
            }

            return (onlyName, "", "", false);
        }

        var name = spec[..nameEnd].Trim();
        var rest = spec[nameEnd..].Trim();
        var typ = "";
        var expr = "";
        var hasExpr = false;

        if (rest.StartsWith("("))
        {
            var closeIdx = rest.IndexOf(")", StringComparison.Ordinal);
            if (closeIdx == -1)
            {
                throw new Exception("missing ')' in type");
            }

            typ = rest[1..closeIdx].Trim();
            rest = rest[(closeIdx + 1)..].Trim();
        }

        if (rest.StartsWith("="))
        {
            rest = rest[1..].Trim();
            if (rest.StartsWith("(") && rest.EndsWith(")"))
            {
                expr = rest[1..^1].Trim();
            }
            else
            {
                expr = rest.Trim();
            }

            hasExpr = expr != "";
        }

        if (name == "" || !EmeraldParser.IsIdent(name))
        {
            throw new Exception("invalid var name");
        }

        return (name, typ, expr, hasExpr);
    }

    private static bool TryParseTypeName(string s, out ValueType t)
    {
        switch (s.Trim().ToLowerInvariant())
        {
            case "int":
                t = ValueType.Int;
                return true;
            case "str":
            case "string":
                t = ValueType.String;
                return true;
            case "bool":
                t = ValueType.Bool;
                return true;
            case "null":
                t = ValueType.Null;
                return true;
            case "list":
                t = ValueType.List;
                return true;
            case "dict":
            case "table":
                t = ValueType.Dict;
                return true;
            default:
                t = ValueType.Unknown;
                return false;
        }
    }

    private static bool Assignable(ValueType target, ValueType expr)
    {
        if (target == ValueType.Unknown)
        {
            return true;
        }

        if (expr == ValueType.Unknown)
        {
            return false;
        }

        if (expr == ValueType.Null)
        {
            return true;
        }

        return target == expr;
    }

    private static (string Name, List<string> Params, List<ValueType> ParamTypes, ValueType RetType) ParseFnDecl(string line)
    {
        var rest = line[4..].Trim();
        if (rest == "")
        {
            throw new Exception("fnc requires a name");
        }

        var name = rest;
        var pars = new List<string>();
        var paramTypes = new List<ValueType>();
        var retType = ValueType.Unknown;
        var i = rest.IndexOf("(", StringComparison.Ordinal);
        if (i != -1)
        {
            var closeIdx = EmeraldParser.FindMatchingParen(rest, i);
            if (closeIdx == -1)
            {
                throw new Exception("fnc params must end with ')' before '{'");
            }

            name = rest[..i].Trim();
            var inner = rest[(i + 1)..closeIdx].Trim();
            if (inner != "")
            {
                var parts = EmeraldParser.SplitTopLevel(inner, ',');
                foreach (var praw in parts)
                {
                    var p = praw.Trim();
                    if (p == "")
                    {
                        continue;
                    }

                    var (pname, ptype) = ParseParamSpec(p);
                    pars.Add(pname);
                    paramTypes.Add(ptype);
                }
            }

            var restAfter = rest[(closeIdx + 1)..].Trim();
            if (restAfter != "")
            {
                if (!restAfter.StartsWith("(") || !restAfter.EndsWith(")"))
                {
                    throw new Exception("fnc return type must be in parentheses");
                }

                var rt = restAfter[1..^1].Trim();
                if (rt == "")
                {
                    throw new Exception("fnc return type is empty");
                }

                if (!TryParseTypeName(rt, out retType))
                {
                    throw new Exception($"unknown type: {rt}");
                }
            }
        }

        if (name == "")
        {
            throw new Exception("fnc requires a name");
        }

        if (!EmeraldParser.IsIdent(name))
        {
            throw new Exception("invalid fnc name");
        }

        if (HasDuplicate(pars))
        {
            throw new Exception("duplicate parameter name");
        }

        return (name, pars, paramTypes, retType);
    }

    private static (string Name, ValueType Type) ParseParamSpec(string spec)
    {
        var (name, typ, _, _) = ParseVarSpec(spec);
        if (typ == "")
        {
            return (name, ValueType.Unknown);
        }

        if (!TryParseTypeName(typ, out var t))
        {
            throw new Exception($"unknown type: {typ}");
        }

        return (name, t);
    }

    private static bool HasDuplicate(List<string> items)
    {
        var seen = new HashSet<string>();
        foreach (var it in items)
        {
            if (!seen.Add(it))
            {
                return true;
            }
        }

        return false;
    }
    private static (string Name, string Expr, bool Ok) ParseForInHeader(string header)
    {
        var inString = false;
        char quote = '\0';
        var parenDepth = 0;
        var bracketDepth = 0;
        for (var i = 0; i < header.Length; i++)
        {
            var ch = header[i];
            if (inString)
            {
                if (ch == quote)
                {
                    inString = false;
                }

                continue;
            }

            switch (ch)
            {
                case '\'':
                case '"':
                    inString = true;
                    quote = ch;
                    break;
                case '(':
                    parenDepth++;
                    break;
                case ')':
                    if (parenDepth > 0)
                    {
                        parenDepth--;
                    }

                    break;
                case '[':
                    bracketDepth++;
                    break;
                case ']':
                    if (bracketDepth > 0)
                    {
                        bracketDepth--;
                    }

                    break;
                default:
                    if (parenDepth == 0 && bracketDepth == 0 && header.AsSpan(i).StartsWith(" in "))
                    {
                        var name = header[..i].Trim();
                        var expr = header[(i + 4)..].Trim();
                        if (EmeraldParser.IsIdent(name) && expr != "")
                        {
                            return (name, expr, true);
                        }

                        return ("", "", false);
                    }

                    break;
            }
        }

        return ("", "", false);
    }

    private static (string Target, string Expr, bool IsAssign) SplitAssignment(string line)
    {
        var inString = false;
        char quote = '\0';
        var parenDepth = 0;
        var bracketDepth = 0;
        for (var i = 0; i < line.Length; i++)
        {
            var ch = line[i];
            if (inString)
            {
                if (ch == quote)
                {
                    inString = false;
                }

                continue;
            }

            switch (ch)
            {
                case '\'':
                case '"':
                    inString = true;
                    quote = ch;
                    break;
                case '(':
                    parenDepth++;
                    break;
                case ')':
                    if (parenDepth > 0)
                    {
                        parenDepth--;
                    }

                    break;
                case '[':
                    bracketDepth++;
                    break;
                case ']':
                    if (bracketDepth > 0)
                    {
                        bracketDepth--;
                    }

                    break;
                case '=':
                    if (parenDepth == 0 && bracketDepth == 0)
                    {
                        if (i + 1 < line.Length && line[i + 1] == '=')
                        {
                            i++;
                            continue;
                        }

                        if (i > 0 && (line[i - 1] == '=' || line[i - 1] == '!'))
                        {
                            continue;
                        }

                        return (line[..i].Trim(), line[(i + 1)..].Trim(), true);
                    }

                    break;
            }
        }

        return ("", "", false);
    }

    private static void SetInstrLine(List<Instr> code, int start, int line)
    {
        for (var i = start; i < code.Count; i++)
        {
            if (code[i].Line == 0)
            {
                code[i].Line = line;
            }
        }
    }

    private void EmitLoad(string name, List<Instr> code, CompileCtx? ctx, int lineNo)
    {
        if (ctx is not null && ctx.InFunction && ctx.Scope is not null && ctx.Scope.Resolve(name, out var slot))
        {
            code.Add(new Instr { Op = OpCode.LoadLocal, Int = slot, Line = lineNo });
            return;
        }

        code.Add(new Instr { Op = OpCode.Load, Str = name, Line = lineNo });
    }

    private void EmitStore(string name, List<Instr> code, CompileCtx? ctx, int lineNo)
    {
        if (ctx is not null && ctx.InFunction && ctx.Scope is not null)
        {
            if (!ctx.Scope.Resolve(name, out var slot))
            {
                slot = ctx.DefineLocal(name);
            }

            code.Add(new Instr { Op = OpCode.StoreLocal, Int = slot, Line = lineNo });
            return;
        }

        code.Add(new Instr { Op = OpCode.Store, Str = name, Line = lineNo });
    }

    private void CompileExpr(string expr, List<Instr> code, TypeEnv? types, CompileCtx? ctx, int lineNo)
    {
        var root = EmeraldParser.ParseExprAst(expr);
        if (types is not null)
        {
            _ = InferExprType(root, types);
        }

        CompileExprNode(root, code, types, ctx, lineNo);
    }

    private ValueType InferExprType(Expr node, TypeEnv types)
    {
        switch (node.Kind)
        {
            case ExprKind.Literal:
                return node.LitKind switch
                {
                    "int" => ValueType.Int,
                    "string" => ValueType.String,
                    "bool" => ValueType.Bool,
                    "null" => ValueType.Null,
                    _ => ValueType.Unknown,
                };
            case ExprKind.Ident:
                return types.Get(node.Name, out var t) ? t : ValueType.Unknown;
            case ExprKind.Unary:
            {
                var inner = InferExprType(node.Left!, types);
                return node.Op switch
                {
                    "not" => ValueType.Bool,
                    "-" when inner == ValueType.Unknown => ValueType.Unknown,
                    "-" when inner == ValueType.Int => ValueType.Int,
                    "-" => throw new Exception("unary '-' expects int"),
                    _ => throw new Exception($"unsupported unary op: {node.Op}"),
                };
            }
            case ExprKind.Binary:
            {
                var lt = InferExprType(node.Left!, types);
                var rt = InferExprType(node.Right!, types);
                return node.Op switch
                {
                    "+" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.Int && rt == ValueType.Int => ValueType.Int,
                    "+" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.List && rt == ValueType.List => ValueType.List,
                    "+" when lt != ValueType.Unknown && rt != ValueType.Unknown && (lt == ValueType.String || rt == ValueType.String) => ValueType.String,
                    "+" when lt != ValueType.Unknown && rt != ValueType.Unknown => throw new Exception("'+' expects int, string, or list"),
                    "+" => ValueType.Unknown,
                    "-" when lt != ValueType.Unknown && lt != ValueType.Int => throw new Exception("'-' expects int"),
                    "-" when rt != ValueType.Unknown && rt != ValueType.Int => throw new Exception("'-' expects int"),
                    "-" when lt == ValueType.Int && rt == ValueType.Int => ValueType.Int,
                    "-" => ValueType.Unknown,
                    "*" when lt != ValueType.Unknown && lt != ValueType.Int => throw new Exception("'*' expects int"),
                    "*" when rt != ValueType.Unknown && rt != ValueType.Int => throw new Exception("'*' expects int"),
                    "*" when lt == ValueType.Int && rt == ValueType.Int => ValueType.Int,
                    "*" => ValueType.Unknown,
                    "/" when lt != ValueType.Unknown && lt != ValueType.Int => throw new Exception("'/' expects int"),
                    "/" when rt != ValueType.Unknown && rt != ValueType.Int => throw new Exception("'/' expects int"),
                    "/" when lt == ValueType.Int && rt == ValueType.Int => ValueType.Int,
                    "/" => ValueType.Unknown,
                    "==" => ValueType.Bool,
                    "!=" => ValueType.Bool,
                    "<" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.Int && rt == ValueType.Int => ValueType.Bool,
                    ">" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.Int && rt == ValueType.Int => ValueType.Bool,
                    "<" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.String && rt == ValueType.String => ValueType.Bool,
                    ">" when lt != ValueType.Unknown && rt != ValueType.Unknown && lt == ValueType.String && rt == ValueType.String => ValueType.Bool,
                    "<" when lt != ValueType.Unknown && rt != ValueType.Unknown => throw new Exception("'<' and '>' expect int or string"),
                    ">" when lt != ValueType.Unknown && rt != ValueType.Unknown => throw new Exception("'<' and '>' expect int or string"),
                    "<" => ValueType.Bool,
                    ">" => ValueType.Bool,
                    "and" => ValueType.Bool,
                    "or" => ValueType.Bool,
                    _ => throw new Exception($"unsupported op: {node.Op}"),
                };
            }
            case ExprKind.Call:
                return InferCallType(node, types);
            case ExprKind.List:
                return ValueType.List;
            case ExprKind.Index:
            {
                var targetType = InferExprType(node.Left!, types);
                if (targetType == ValueType.String)
                {
                    return ValueType.String;
                }

                return ValueType.Unknown;
            }
            case ExprKind.Slice:
            {
                var targetType = InferExprType(node.Target!, types);
                if (targetType != ValueType.Unknown && targetType != ValueType.List && targetType != ValueType.String)
                {
                    throw new Exception("slice expects list or string");
                }

                if (targetType == ValueType.String)
                {
                    return ValueType.String;
                }

                if (targetType == ValueType.List)
                {
                    return ValueType.List;
                }

                return ValueType.Unknown;
            }
            default:
                throw new Exception("unknown expr");
        }
    }

    private ValueType InferCallType(Expr node, TypeEnv types)
    {
        switch (node.Name)
        {
            case "input":
                if (node.Args.Count > 1)
                {
                    throw new Exception("input expects 0 or 1 argument");
                }

                return ValueType.String;
            case "plc":
                if (node.Args.Count != 1)
                {
                    throw new Exception("plc expects 1 argument");
                }

                return ValueType.String;
            case "read":
                if (node.Args.Count > 1)
                {
                    throw new Exception("read expects 0 or 1 argument");
                }

                return ValueType.String;
            case "len":
                if (node.Args.Count != 1)
                {
                    throw new Exception("len expects 1 argument");
                }

                return ValueType.Int;
            case "append":
                if (node.Args.Count != 2)
                {
                    throw new Exception("append expects 2 arguments");
                }

                var lt = InferExprType(node.Args[0], types);
                if (lt != ValueType.Unknown && lt != ValueType.List)
                {
                    throw new Exception("append expects list as first argument");
                }

                return ValueType.List;
            case "keys":
                if (node.Args.Count != 1)
                {
                    throw new Exception("keys expects 1 argument");
                }

                var dt = InferExprType(node.Args[0], types);
                if (dt != ValueType.Unknown && dt != ValueType.Dict)
                {
                    throw new Exception("keys expects dict");
                }

                return ValueType.List;
            case "math_abs":
                if (node.Args.Count != 1)
                {
                    throw new Exception("math_abs expects 1 argument");
                }

                return ValueType.Int;
            case "math_min":
            case "math_max":
            case "math_pow":
                if (node.Args.Count != 2)
                {
                    throw new Exception($"{node.Name} expects 2 arguments");
                }

                return ValueType.Int;
            case "table":
            case "dict":
                return ValueType.Dict;
            default:
                if (!Functions.TryGetValue(node.Name, out var fn))
                {
                    return ValueType.Unknown;
                }

                if (node.Args.Count != fn.Params.Count)
                {
                    throw new Exception($"function {node.Name} expects {fn.Params.Count} args, got {node.Args.Count}");
                }

                for (var i = 0; i < node.Args.Count; i++)
                {
                    var at = InferExprType(node.Args[i], types);
                    if (i < fn.ParamTypes.Count && fn.ParamTypes[i] != ValueType.Unknown)
                    {
                        if (at == ValueType.Unknown)
                        {
                            throw new Exception($"type mismatch: {node.Name} arg {i + 1} is {fn.ParamTypes[i].ToTypeString()}, got {at.ToTypeString()}");
                        }

                        if (!Assignable(fn.ParamTypes[i], at))
                        {
                            throw new Exception($"type mismatch: {node.Name} arg {i + 1} is {fn.ParamTypes[i].ToTypeString()}, got {at.ToTypeString()}");
                        }
                    }
                }

                return fn.ReturnType == ValueType.Unknown ? ValueType.Unknown : fn.ReturnType;
        }
    }

    private void CompileExprNode(Expr node, List<Instr> code, TypeEnv? types, CompileCtx? ctx, int lineNo)
    {
        switch (node.Kind)
        {
            case ExprKind.Literal:
                switch (node.LitKind)
                {
                    case "int":
                        code.Add(new Instr { Op = OpCode.PushInt, Int = node.I, Line = lineNo });
                        break;
                    case "string":
                        code.Add(new Instr { Op = OpCode.PushStr, Str = node.S, Line = lineNo });
                        break;
                    case "bool":
                        code.Add(new Instr { Op = OpCode.PushBool, Bool = node.B, Line = lineNo });
                        break;
                    case "null":
                        code.Add(new Instr { Op = OpCode.PushNull, Line = lineNo });
                        break;
                    default:
                        throw new Exception("unknown literal");
                }

                return;
            case ExprKind.Ident:
                EmitLoad(node.Name, code, ctx, lineNo);
                return;
            case ExprKind.Unary:
                CompileExprNode(node.Left!, code, types, ctx, lineNo);
                switch (node.Op)
                {
                    case "not":
                        code.Add(new Instr { Op = OpCode.Not, Line = lineNo });
                        break;
                    case "-":
                        code.Add(new Instr { Op = OpCode.Neg, Line = lineNo });
                        break;
                    default:
                        throw new Exception($"unsupported unary op: {node.Op}");
                }

                return;
            case ExprKind.Binary:
                CompileExprNode(node.Left!, code, types, ctx, lineNo);
                CompileExprNode(node.Right!, code, types, ctx, lineNo);
                code.Add(new Instr
                {
                    Op = node.Op switch
                    {
                        "+" => OpCode.Add,
                        "-" => OpCode.Sub,
                        "*" => OpCode.Mul,
                        "/" => OpCode.Div,
                        "==" => OpCode.Eq,
                        "!=" => OpCode.Neq,
                        "<" => OpCode.Lt,
                        ">" => OpCode.Gt,
                        "and" => OpCode.And,
                        "or" => OpCode.Or,
                        _ => throw new Exception($"unsupported op: {node.Op}"),
                    },
                    Line = lineNo,
                });
                return;
            case ExprKind.Call:
                foreach (var arg in node.Args)
                {
                    CompileExprNode(arg, code, types, ctx, lineNo);
                }

                code.Add(new Instr { Op = OpCode.Call, Str = node.Name, Int = node.Args.Count, Line = lineNo });
                return;
            case ExprKind.List:
                foreach (var e in node.Elements)
                {
                    CompileExprNode(e, code, types, ctx, lineNo);
                }

                code.Add(new Instr { Op = OpCode.MakeList, Int = node.Elements.Count, Line = lineNo });
                return;
            case ExprKind.Index:
                CompileExprNode(node.Left!, code, types, ctx, lineNo);
                CompileExprNode(node.Right!, code, types, ctx, lineNo);
                code.Add(new Instr { Op = OpCode.IndexGet, Line = lineNo });
                return;
            case ExprKind.Slice:
                CompileExprNode(node.Target!, code, types, ctx, lineNo);
                if (node.Start is not null)
                {
                    CompileExprNode(node.Start, code, types, ctx, lineNo);
                }
                else
                {
                    code.Add(new Instr { Op = OpCode.PushNull, Line = lineNo });
                }

                if (node.End is not null)
                {
                    CompileExprNode(node.End, code, types, ctx, lineNo);
                }
                else
                {
                    code.Add(new Instr { Op = OpCode.PushNull, Line = lineNo });
                }

                code.Add(new Instr { Op = OpCode.Slice, Line = lineNo });
                return;
            default:
                throw new Exception("unknown expr");
        }
    }
}

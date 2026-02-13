using System.Text;
using System.Text.Json.Serialization;

namespace mycoolapp;

internal enum EmeraldCommand
{
    Build,
    Run,
    Shell,
    Touch,
    Carve,
    Shine,
}

internal sealed record CliOptions(EmeraldCommand Command, string Path);

internal sealed record ParseResult(bool Success, CliOptions? Options, IReadOnlyList<string> Messages);

internal sealed record CommandResult(bool Success, string Message)
{
    public static CommandResult Ok(string message = "") => new(true, message);

    public static CommandResult Fail(string message) => new(false, message);
}

internal enum OpCode : byte
{
    Noop,
    PushInt,
    PushStr,
    PushBool,
    PushNull,
    Load,
    Store,
    Print,
    Input,
    Wait,
    Check,
    Jmp,
    JmpIfFalse,
    Call,
    Ret,
    Pop,
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Lt,
    Gt,
    And,
    Or,
    Not,
    Neg,
    MakeList,
    LoadLocal,
    StoreLocal,
    IndexGet,
    IndexSet,
    Slice,
}

internal sealed class Instr
{
    public OpCode Op { get; set; }

    public string Str { get; set; } = "";

    public int Int { get; set; }

    public bool Bool { get; set; }

    public int Line { get; set; }
}

internal enum ValueType
{
    Unknown,
    Int,
    String,
    Bool,
    Null,
    List,
    Dict,
}

internal static class ValueTypeExtensions
{
    public static string ToTypeString(this ValueType t)
    {
        return t switch
        {
            ValueType.Int => "int",
            ValueType.String => "string",
            ValueType.Bool => "bool",
            ValueType.Null => "null",
            ValueType.List => "list",
            ValueType.Dict => "dict",
            _ => "unknown",
        };
    }
}

internal sealed class FunctionDef
{
    public List<string> Params { get; set; } = [];

    public List<ValueType> ParamTypes { get; set; } = [];

    public ValueType ReturnType { get; set; } = ValueType.Unknown;

    public int LocalCount { get; set; }

    public List<Instr> Code { get; set; } = [];
}

internal sealed class ProgramModel
{
    public int Version { get; set; }

    public List<Instr> Main { get; set; } = [];

    public Dictionary<string, FunctionDef> Functions { get; set; } = [];
}

internal enum TokenKind
{
    Line,
    LBrace,
    RBrace,
}

internal sealed class Token
{
    public TokenKind Kind { get; set; }

    public string Text { get; set; } = "";

    public int Line { get; set; }
}

internal sealed class LocalScope
{
    private readonly Dictionary<string, int> _vars = [];
    private readonly LocalScope? _parent;

    public LocalScope(LocalScope? parent)
    {
        _parent = parent;
    }

    public bool Resolve(string name, out int slot)
    {
        for (var cur = this; cur is not null; cur = cur._parent)
        {
            if (cur._vars.TryGetValue(name, out slot))
            {
                return true;
            }
        }

        slot = 0;
        return false;
    }

    public bool GetLocal(string name, out int slot) => _vars.TryGetValue(name, out slot);

    public void SetLocal(string name, int slot) => _vars[name] = slot;
}

internal sealed class CompileCtx
{
    public bool InFunction { get; set; }
    public LocalScope? Scope { get; set; }
    public int NextSlot { get; set; }

    public CompileCtx ChildScope()
    {
        return new CompileCtx
        {
            InFunction = InFunction,
            Scope = new LocalScope(Scope),
            NextSlot = NextSlot,
        };
    }

    public int DefineLocal(string name)
    {
        if (Scope is null)
        {
            return -1;
        }

        if (Scope.GetLocal(name, out var slot))
        {
            return slot;
        }

        slot = NextSlot;
        NextSlot++;
        Scope.SetLocal(name, slot);
        return slot;
    }
}

internal sealed class TypeEnv
{
    private readonly Dictionary<string, ValueType> _vars = [];
    private readonly TypeEnv? _parent;

    public TypeEnv(TypeEnv? parent)
    {
        _parent = parent;
    }

    public bool Get(string name, out ValueType value)
    {
        for (var cur = this; cur is not null; cur = cur._parent)
        {
            if (cur._vars.TryGetValue(name, out value))
            {
                return true;
            }
        }

        value = ValueType.Unknown;
        return false;
    }

    public bool GetLocal(string name, out ValueType value) => _vars.TryGetValue(name, out value);

    public void SetLocal(string name, ValueType value) => _vars[name] = value;
}

internal sealed class LoopCtx
{
    public int Start { get; set; }
    public List<int> BreakJumps { get; } = [];
    public List<int> ContinueJumps { get; } = [];
}

internal enum ExprTokenKind
{
    Eof,
    Ident,
    Number,
    String,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Op,
}

internal readonly record struct ExprToken(ExprTokenKind Kind, string Text);

internal enum ExprKind
{
    Literal,
    Ident,
    Unary,
    Binary,
    Call,
    List,
    Index,
    Slice,
}

internal sealed class Expr
{
    public ExprKind Kind { get; set; }
    public string LitKind { get; set; } = "";
    public int I { get; set; }
    public string S { get; set; } = "";
    public bool B { get; set; }
    public string Name { get; set; } = "";
    public string Op { get; set; } = "";
    public Expr? Left { get; set; }
    public Expr? Right { get; set; }
    public Expr? Target { get; set; }
    public Expr? Start { get; set; }
    public Expr? End { get; set; }
    public List<Expr> Args { get; set; } = [];
    public List<Expr> Elements { get; set; } = [];
}

internal sealed class Value
{
    public string Kind { get; set; } = "null";
    public int I { get; set; }
    public string S { get; set; } = "";
    public bool B { get; set; }
    public List<Value> L { get; set; } = [];
    public Dictionary<string, Value> M { get; set; } = [];

    public static Value Null() => new() { Kind = "null" };
    public static Value Int(int v) => new() { Kind = "int", I = v };
    public static Value Str(string v) => new() { Kind = "string", S = v };
    public static Value Bool(bool v) => new() { Kind = "bool", B = v };
    public static Value List(List<Value> v) => new() { Kind = "list", L = v };
    public static Value Dict(Dictionary<string, Value> v) => new() { Kind = "dict", M = v };

    public bool Truthy()
    {
        return Kind switch
        {
            "null" => false,
            "bool" => B,
            "int" => I != 0,
            "string" => S != "",
            "list" => L.Count > 0,
            "dict" => M.Count > 0,
            _ => false,
        };
    }

    public int AsInt()
    {
        return Kind switch
        {
            "int" => I,
            "bool" => B ? 1 : 0,
            "string" when int.TryParse(S, out var i) => i,
            _ => 0,
        };
    }

    public bool Equal(Value o)
    {
        if (Kind != o.Kind)
        {
            return false;
        }

        switch (Kind)
        {
            case "null":
                return true;
            case "bool":
                return B == o.B;
            case "int":
                return I == o.I;
            case "string":
                return S == o.S;
            case "list":
                if (L.Count != o.L.Count)
                {
                    return false;
                }

                for (var i = 0; i < L.Count; i++)
                {
                    if (!L[i].Equal(o.L[i]))
                    {
                        return false;
                    }
                }

                return true;
            case "dict":
                if (M.Count != o.M.Count)
                {
                    return false;
                }

                foreach (var (k, v) in M)
                {
                    if (!o.M.TryGetValue(k, out var ov) || !v.Equal(ov))
                    {
                        return false;
                    }
                }

                return true;
            default:
                return false;
        }
    }

    public string ToValueString()
    {
        return Kind switch
        {
            "null" => "null",
            "bool" => B ? "true" : "false",
            "int" => I.ToString(),
            "string" => S,
            "list" => "[" + string.Join(", ", L.Select(x => x.ToValueString())) + "]",
            "dict" => "{" + string.Join(", ", M.Select(kv => kv.Key + ": " + kv.Value.ToValueString())) + "}",
            _ => "",
        };
    }
}

internal sealed class TraceFrame
{
    public string Function { get; set; } = "";
    public int Line { get; set; }
}

internal sealed class RuntimeError : Exception
{
    [JsonInclude]
    public List<TraceFrame> Frames { get; private set; } = [];

    public RuntimeError(string message)
        : base(message)
    {
    }

    public RuntimeError(string message, List<TraceFrame> frames)
        : base(message)
    {
        Frames = frames;
    }

    public override string ToString()
    {
        var b = new StringBuilder();
        b.Append(Message);
        if (Frames.Count > 0)
        {
            b.Append("\nTraceback:");
            foreach (var f in Frames)
            {
                if (f.Line > 0)
                {
                    b.Append($"\n  at {f.Function} (line {f.Line})");
                }
                else
                {
                    b.Append($"\n  at {f.Function}");
                }
            }
        }

        return b.ToString();
    }
}

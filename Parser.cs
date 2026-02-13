using System.Text;

namespace mycoolapp;

internal static class CliParser
{
    public static ParseResult Parse(string[] args)
    {
        if (args.Length < 1)
        {
            return new ParseResult(false, null, UsageLines());
        }

        var commandText = args[0].Trim().ToLowerInvariant();

        return commandText switch
        {
            "build" => ParseBuild(args),
            "run" => ParseRun(args),
            "shell" => new ParseResult(true, new CliOptions(EmeraldCommand.Shell, ""), []),
            "touch" => ParseEmerPathArg(args, EmeraldCommand.Touch, "touch <file>.emer"),
            "carve" => ParseEmerPathArg(args, EmeraldCommand.Carve, "carve <file>.emer"),
            "shine" => ParseEmerPathArg(args, EmeraldCommand.Shine, "shine <file>.emer"),
            _ => new ParseResult(false, null, new[]
            {
                $"Unknown command: {commandText}",
                "Usage:",
                "  emerald build <file.emer>",
                "  emerald run <file.emer|file.emec>",
                "  emerald shell",
                "  emerald touch <file>.emer",
                "  emerald carve <file>.emer",
                "  emerald shine <file>.emer",
            }),
        };
    }

    private static ParseResult ParseEmerPathArg(string[] args, EmeraldCommand command, string usage)
    {
        if (args.Length < 2)
        {
            return new ParseResult(false, null, [$"usage: {usage}"]);
        }

        var path = args[1].Trim();
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase))
        {
            return new ParseResult(false, null, ["file must end with .emer"]);
        }

        return new ParseResult(true, new CliOptions(command, path), []);
    }

    private static ParseResult ParseBuild(string[] args)
    {
        if (args.Length < 2)
        {
            return new ParseResult(false, null, ["build expects a .emer source file"]);
        }

        var path = args[1].Trim();
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase))
        {
            return new ParseResult(false, null, ["build expects a .emer source file"]);
        }

        return new ParseResult(true, new CliOptions(EmeraldCommand.Build, path), []);
    }

    private static ParseResult ParseRun(string[] args)
    {
        if (args.Length < 2)
        {
            return new ParseResult(false, null, ["run expects a .emer or .emec file"]);
        }

        var path = args[1].Trim();
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase) &&
            !path.EndsWith(".emec", StringComparison.OrdinalIgnoreCase))
        {
            return new ParseResult(false, null, ["run expects a .emer or .emec file"]);
        }

        return new ParseResult(true, new CliOptions(EmeraldCommand.Run, path), []);
    }

    private static string[] UsageLines()
    {
        return
        [
            "Usage:",
            "  emerald build <file.emer>",
            "  emerald run <file.emer|file.emec>",
            "  emerald shell",
            "  emerald touch <file>.emer",
            "  emerald carve <file>.emer",
            "  emerald shine <file>.emer",
        ];
    }
}

internal static class EmeraldParser
{
    public static List<Token> Tokenize(string src)
    {
        var tokens = new List<Token>();
        var line = new StringBuilder();
        var inString = false;
        char quote = '\0';
        var lineNum = 1;

        void FlushLine()
        {
            var s = line.ToString().Trim();
            if (s != "")
            {
                tokens.Add(new Token { Kind = TokenKind.Line, Text = s, Line = lineNum });
            }

            line.Clear();
        }

        foreach (var r in src)
        {
            if (inString)
            {
                line.Append(r);
                if (r == quote)
                {
                    inString = false;
                }

                continue;
            }

            switch (r)
            {
                case '\'':
                case '"':
                    inString = true;
                    quote = r;
                    line.Append(r);
                    break;
                case '{':
                    FlushLine();
                    tokens.Add(new Token { Kind = TokenKind.LBrace, Text = "{", Line = lineNum });
                    break;
                case '}':
                    FlushLine();
                    tokens.Add(new Token { Kind = TokenKind.RBrace, Text = "}", Line = lineNum });
                    break;
                case '\n':
                    FlushLine();
                    lineNum++;
                    break;
                case '\r':
                    break;
                default:
                    line.Append(r);
                    break;
            }
        }

        FlushLine();
        return tokens;
    }

    public static bool IsIdent(string s)
    {
        if (s == "")
        {
            return false;
        }

        for (var i = 0; i < s.Length; i++)
        {
            var r = s[i];
            if (i == 0)
            {
                if (!(r == '_' || IsLetter(r)))
                {
                    return false;
                }

                continue;
            }

            if (!(r == '_' || IsLetter(r) || IsDigit(r)))
            {
                return false;
            }
        }

        return true;
    }

    public static (string Value, bool Ok) ParseStringLiteral(string s)
    {
        if (s.Length >= 2 &&
            ((s[0] == '"' && s[^1] == '"') || (s[0] == '\'' && s[^1] == '\'')))
        {
            return (s[1..^1], true);
        }

        return ("", false);
    }

    public static int FindMatchingParen(string s, int openIdx)
    {
        if (openIdx < 0 || openIdx >= s.Length || s[openIdx] != '(')
        {
            return -1;
        }

        var depth = 0;
        for (var i = openIdx; i < s.Length; i++)
        {
            switch (s[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }

                    break;
            }
        }

        return -1;
    }

    public static List<string> SplitTopLevel(string s, char sep)
    {
        var parts = new List<string>();
        var cur = new StringBuilder();
        var inString = false;
        char quote = '\0';
        var parenDepth = 0;
        var bracketDepth = 0;

        foreach (var r in s)
        {
            if (inString)
            {
                cur.Append(r);
                if (r == quote)
                {
                    inString = false;
                }

                continue;
            }

            switch (r)
            {
                case '\'':
                case '"':
                    inString = true;
                    quote = r;
                    cur.Append(r);
                    break;
                case '(':
                    parenDepth++;
                    cur.Append(r);
                    break;
                case ')':
                    if (parenDepth > 0)
                    {
                        parenDepth--;
                    }

                    cur.Append(r);
                    break;
                case '[':
                    bracketDepth++;
                    cur.Append(r);
                    break;
                case ']':
                    if (bracketDepth > 0)
                    {
                        bracketDepth--;
                    }

                    cur.Append(r);
                    break;
                default:
                    if (r == sep && parenDepth == 0 && bracketDepth == 0)
                    {
                        parts.Add(cur.ToString());
                        cur.Clear();
                    }
                    else
                    {
                        cur.Append(r);
                    }

                    break;
            }
        }

        if (cur.Length > 0)
        {
            parts.Add(cur.ToString());
        }

        return parts;
    }

    public static Expr ParseExprAst(string expr)
    {
        var tokens = LexExpr(expr);
        var p = new ExprParser(tokens);
        var root = p.ParseExpr();
        if (p.Peek().Kind != ExprTokenKind.Eof)
        {
            throw new Exception("unexpected trailing tokens");
        }

        return root;
    }

    public static List<ExprToken> LexExpr(string s)
    {
        var tokens = new List<ExprToken>();
        for (var i = 0; i < s.Length;)
        {
            var r = s[i];
            if (char.IsWhiteSpace(r))
            {
                i++;
                continue;
            }

            if (r == 'f' && i + 1 < s.Length && (s[i + 1] == '"' || s[i + 1] == '\''))
            {
                var (ftoks, next) = LexFString(s, i);
                tokens.AddRange(ftoks);
                i = next;
                continue;
            }

            if (IsLetter(r) || r == '_')
            {
                var start = i;
                i++;
                while (i < s.Length && (IsLetter(s[i]) || IsDigit(s[i]) || s[i] == '_'))
                {
                    i++;
                }

                var text = s[start..i];
                if (text is "and" or "or" or "not")
                {
                    tokens.Add(new ExprToken(ExprTokenKind.Op, text));
                }
                else
                {
                    tokens.Add(new ExprToken(ExprTokenKind.Ident, text));
                }

                continue;
            }

            if (IsDigit(r))
            {
                var start = i;
                i++;
                while (i < s.Length && IsDigit(s[i]))
                {
                    i++;
                }

                tokens.Add(new ExprToken(ExprTokenKind.Number, s[start..i]));
                continue;
            }

            if (r == '\'' || r == '"')
            {
                var quote = r;
                var start = i + 1;
                i++;
                while (i < s.Length && s[i] != quote)
                {
                    i++;
                }

                if (i >= s.Length)
                {
                    throw new Exception("unterminated string");
                }

                tokens.Add(new ExprToken(ExprTokenKind.String, s[start..i]));
                i++;
                continue;
            }

            switch (r)
            {
                case '(':
                    tokens.Add(new ExprToken(ExprTokenKind.LParen, "("));
                    i++;
                    break;
                case ')':
                    tokens.Add(new ExprToken(ExprTokenKind.RParen, ")"));
                    i++;
                    break;
                case '[':
                    tokens.Add(new ExprToken(ExprTokenKind.LBracket, "["));
                    i++;
                    break;
                case ']':
                    tokens.Add(new ExprToken(ExprTokenKind.RBracket, "]"));
                    i++;
                    break;
                case ',':
                    tokens.Add(new ExprToken(ExprTokenKind.Comma, ","));
                    i++;
                    break;
                case ':':
                    tokens.Add(new ExprToken(ExprTokenKind.Colon, ":"));
                    i++;
                    break;
                case '+':
                case '-':
                case '*':
                case '/':
                case '<':
                case '>':
                    tokens.Add(new ExprToken(ExprTokenKind.Op, r.ToString()));
                    i++;
                    break;
                case '=':
                    if (i + 1 < s.Length && s[i + 1] == '=')
                    {
                        tokens.Add(new ExprToken(ExprTokenKind.Op, "=="));
                        i += 2;
                    }
                    else
                    {
                        throw new Exception("unexpected '='");
                    }

                    break;
                case '!':
                    if (i + 1 < s.Length && s[i + 1] == '=')
                    {
                        tokens.Add(new ExprToken(ExprTokenKind.Op, "!="));
                        i += 2;
                    }
                    else
                    {
                        throw new Exception("unexpected '!'");
                    }

                    break;
                default:
                    throw new Exception($"unexpected character: {r}");
            }
        }

        tokens.Add(new ExprToken(ExprTokenKind.Eof, ""));
        return tokens;
    }

    public static (List<ExprToken> Tokens, int Next) LexFString(string s, int start)
    {
        if (start + 1 >= s.Length)
        {
            throw new Exception("unterminated f-string");
        }

        var quote = s[start + 1];
        var i = start + 2;
        var parts = new List<List<ExprToken>>();
        var lit = new StringBuilder();
        var foundPart = false;

        while (i < s.Length)
        {
            var ch = s[i];
            if (ch == quote)
            {
                i++;
                break;
            }

            if (ch == '{')
            {
                if (lit.Length > 0)
                {
                    parts.Add([new ExprToken(ExprTokenKind.String, lit.ToString())]);
                    lit.Clear();
                    foundPart = true;
                }

                var end = i + 1;
                var depth = 1;
                while (end < s.Length)
                {
                    if (s[end] == '{')
                    {
                        depth++;
                    }
                    else if (s[end] == '}')
                    {
                        depth--;
                        if (depth == 0)
                        {
                            break;
                        }
                    }

                    end++;
                }

                if (end >= s.Length || s[end] != '}')
                {
                    throw new Exception("unterminated f-string expression");
                }

                var inner = s[(i + 1)..end].Trim();
                if (inner == "")
                {
                    throw new Exception("empty f-string expression");
                }

                var exprTokens = LexExpr(inner);
                if (exprTokens.Count > 0)
                {
                    exprTokens.RemoveAt(exprTokens.Count - 1);
                }

                var part = new List<ExprToken> { new(ExprTokenKind.LParen, "(") };
                part.AddRange(exprTokens);
                part.Add(new ExprToken(ExprTokenKind.RParen, ")"));
                parts.Add(part);
                foundPart = true;
                i = end + 1;
                continue;
            }

            lit.Append(ch);
            i++;
        }

        if (i >= s.Length && (s.Length == 0 || s[i - 1] != quote))
        {
            throw new Exception("unterminated f-string");
        }

        if (lit.Length > 0)
        {
            parts.Add([new ExprToken(ExprTokenKind.String, lit.ToString())]);
            foundPart = true;
        }

        if (!foundPart)
        {
            parts.Add([new ExprToken(ExprTokenKind.String, "")]);
        }

        var output = new List<ExprToken>();
        for (var idx = 0; idx < parts.Count; idx++)
        {
            if (idx > 0)
            {
                output.Add(new ExprToken(ExprTokenKind.Op, "+"));
            }

            output.AddRange(parts[idx]);
        }

        return (output, i);
    }

    public static bool IsLetter(char b) => (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z');

    public static bool IsDigit(char b) => b >= '0' && b <= '9';
}

internal sealed class ExprParser
{
    private readonly List<ExprToken> _tokens;
    private int _pos;

    public ExprParser(List<ExprToken> tokens)
    {
        _tokens = tokens;
    }

    public ExprToken Peek()
    {
        if (_pos >= _tokens.Count)
        {
            return new ExprToken(ExprTokenKind.Eof, "");
        }

        return _tokens[_pos];
    }

    private ExprToken Next()
    {
        var t = Peek();
        if (_pos < _tokens.Count)
        {
            _pos++;
        }

        return t;
    }

    private void Expect(ExprTokenKind kind, string text = "")
    {
        var t = Next();
        if (t.Kind != kind || (text != "" && t.Text != text))
        {
            throw new Exception($"unexpected token: {t.Text}");
        }
    }

    public Expr ParseExpr() => ParseOr();

    private Expr ParseOr()
    {
        var left = ParseAnd();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && t.Text == "or")
            {
                _ = Next();
                var right = ParseAnd();
                left = new Expr { Kind = ExprKind.Binary, Op = "or", Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseAnd()
    {
        var left = ParseEquality();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && t.Text == "and")
            {
                _ = Next();
                var right = ParseEquality();
                left = new Expr { Kind = ExprKind.Binary, Op = "and", Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseEquality()
    {
        var left = ParseCompare();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && (t.Text == "==" || t.Text == "!="))
            {
                _ = Next();
                var right = ParseCompare();
                left = new Expr { Kind = ExprKind.Binary, Op = t.Text, Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseCompare()
    {
        var left = ParseTerm();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && (t.Text == "<" || t.Text == ">"))
            {
                _ = Next();
                var right = ParseTerm();
                left = new Expr { Kind = ExprKind.Binary, Op = t.Text, Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseTerm()
    {
        var left = ParseFactor();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && (t.Text == "+" || t.Text == "-"))
            {
                _ = Next();
                var right = ParseFactor();
                left = new Expr { Kind = ExprKind.Binary, Op = t.Text, Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseFactor()
    {
        var left = ParseUnary();
        while (true)
        {
            var t = Peek();
            if (t.Kind == ExprTokenKind.Op && (t.Text == "*" || t.Text == "/"))
            {
                _ = Next();
                var right = ParseUnary();
                left = new Expr { Kind = ExprKind.Binary, Op = t.Text, Left = left, Right = right };
                continue;
            }

            break;
        }

        return left;
    }

    private Expr ParseUnary()
    {
        var t = Peek();
        if (t.Kind == ExprTokenKind.Op && (t.Text == "not" || t.Text == "-"))
        {
            _ = Next();
            var expr = ParseUnary();
            return new Expr { Kind = ExprKind.Unary, Op = t.Text, Left = expr };
        }

        return ParsePostfix();
    }

    private Expr ParsePostfix()
    {
        var node = ParseAtom();
        while (true)
        {
            switch (Peek().Kind)
            {
                case ExprTokenKind.LParen:
                    _ = Next();
                    var args = new List<Expr>();
                    if (Peek().Kind != ExprTokenKind.RParen)
                    {
                        while (true)
                        {
                            args.Add(ParseExpr());
                            if (Peek().Kind == ExprTokenKind.Comma)
                            {
                                _ = Next();
                                continue;
                            }

                            break;
                        }
                    }

                    Expect(ExprTokenKind.RParen);
                    if (node.Kind != ExprKind.Ident)
                    {
                        throw new Exception("only function identifiers are callable");
                    }

                    node = new Expr { Kind = ExprKind.Call, Name = node.Name, Args = args };
                    break;
                case ExprTokenKind.LBracket:
                    _ = Next();
                    if (Peek().Kind == ExprTokenKind.RBracket)
                    {
                        throw new Exception("empty index is not allowed");
                    }

                    Expr? first = null;
                    Expr? second = null;
                    var hasColon = false;
                    if (Peek().Kind != ExprTokenKind.Colon)
                    {
                        first = ParseOr();
                    }

                    if (Peek().Kind == ExprTokenKind.Colon)
                    {
                        hasColon = true;
                        _ = Next();
                        if (Peek().Kind != ExprTokenKind.RBracket)
                        {
                            second = ParseOr();
                        }
                    }

                    Expect(ExprTokenKind.RBracket);
                    if (hasColon)
                    {
                        node = new Expr { Kind = ExprKind.Slice, Target = node, Start = first, End = second };
                    }
                    else
                    {
                        node = new Expr { Kind = ExprKind.Index, Left = node, Right = first };
                    }

                    break;
                default:
                    return node;
            }
        }
    }

    private Expr ParseAtom()
    {
        var t = Next();
        switch (t.Kind)
        {
            case ExprTokenKind.Number:
                _ = int.TryParse(t.Text, out var i);
                return new Expr { Kind = ExprKind.Literal, LitKind = "int", I = i };
            case ExprTokenKind.String:
                return new Expr { Kind = ExprKind.Literal, LitKind = "string", S = t.Text };
            case ExprTokenKind.Ident:
                return t.Text switch
                {
                    "true" => new Expr { Kind = ExprKind.Literal, LitKind = "bool", B = true },
                    "false" => new Expr { Kind = ExprKind.Literal, LitKind = "bool", B = false },
                    "null" => new Expr { Kind = ExprKind.Literal, LitKind = "null" },
                    _ => new Expr { Kind = ExprKind.Ident, Name = t.Text },
                };
            case ExprTokenKind.LParen:
            {
                var expr = ParseExpr();
                Expect(ExprTokenKind.RParen);
                return expr;
            }
            case ExprTokenKind.LBracket:
            {
                var elems = new List<Expr>();
                if (Peek().Kind != ExprTokenKind.RBracket)
                {
                    while (true)
                    {
                        elems.Add(ParseExpr());
                        if (Peek().Kind == ExprTokenKind.Comma)
                        {
                            _ = Next();
                            continue;
                        }

                        break;
                    }
                }

                Expect(ExprTokenKind.RBracket);
                return new Expr { Kind = ExprKind.List, Elements = elems };
            }
            default:
                throw new Exception($"unexpected token: {t.Text}");
        }
    }
}

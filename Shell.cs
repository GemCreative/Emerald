using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;

namespace mycoolapp;

internal static class EmeraldShell
{
    public static CommandResult TouchCommand(string path)
    {
        var err = Touch(path);
        return err is null ? CommandResult.Ok() : CommandResult.Fail(err);
    }

    public static CommandResult CarveCommand(string path)
    {
        var err = Carve(path);
        return err is null ? CommandResult.Ok() : CommandResult.Fail(err);
    }

    public static CommandResult ShineCommand(string path)
    {
        var err = Shine(path);
        return err is null ? CommandResult.Ok() : CommandResult.Fail(err);
    }

    public static CommandResult Run()
    {
        try
        {
            var cwd = Directory.GetCurrentDirectory();
            Console.WriteLine("Emerald Shell");
            Console.WriteLine($"cwd: {cwd}");

            while (true)
            {
                Console.Write("emer-shell> ");
                var line = (Console.ReadLine() ?? "").Trim();
                if (line == "")
                {
                    continue;
                }

                if (line is "exit" or "quit")
                {
                    return CommandResult.Ok();
                }

                var parts = SplitArgs(line);
                if (parts.Count == 0)
                {
                    continue;
                }

                switch (parts[0])
                {
                    case "touch":
                        if (parts.Count != 2)
                        {
                            Console.WriteLine("usage: touch <file>.emer");
                            continue;
                        }

                        PrintErrorIfAny(Touch(parts[1]));
                        break;
                    case "carve":
                        if (parts.Count != 2)
                        {
                            Console.WriteLine("usage: carve <file>.emer");
                            continue;
                        }

                        PrintErrorIfAny(Carve(parts[1]));
                        break;
                    case "shine":
                        if (parts.Count != 2)
                        {
                            Console.WriteLine("usage: shine <file>.emer");
                            continue;
                        }

                        PrintErrorIfAny(Shine(parts[1]));
                        break;
                    default:
                        Console.WriteLine($"unknown command: {parts[0]}");
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            return CommandResult.Fail(ex.Message);
        }
    }

    private static void PrintErrorIfAny(string? err)
    {
        if (!string.IsNullOrWhiteSpace(err))
        {
            Console.WriteLine($"error: {err}");
        }
    }

    private static string? Touch(string path)
    {
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase))
        {
            return "file must end with .emer";
        }

        try
        {
            using var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private static string? Carve(string path)
    {
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase))
        {
            return "file must end with .emer";
        }

        var editor = Environment.GetEnvironmentVariable("EDITOR");
        if (string.IsNullOrWhiteSpace(editor))
        {
            editor = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "notepad" : "vi";
        }

        try
        {
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = editor,
                Arguments = $"\"{path}\"",
                UseShellExecute = false,
            });
            process?.WaitForExit();
            return process is null ? "failed to start editor" : null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    private static string? Shine(string path)
    {
        if (!path.EndsWith(".emer", StringComparison.OrdinalIgnoreCase))
        {
            return "file must end with .emer";
        }

        var build = EmeraldCompiler.Build(new CliOptions(EmeraldCommand.Build, path));
        if (!build.Success)
        {
            return build.Message;
        }

        var emec = Path.ChangeExtension(path, ".emec")!;
        var run = EmeraldVm.Run(new CliOptions(EmeraldCommand.Run, emec));
        if (!run.Success)
        {
            return run.Message;
        }

        return null;
    }

    private static List<string> SplitArgs(string s)
    {
        var output = new List<string>();
        var cur = new StringBuilder();
        var inQuote = false;
        char quote = '\0';

        foreach (var r in s)
        {
            if (inQuote)
            {
                if (r == quote)
                {
                    inQuote = false;
                }
                else
                {
                    cur.Append(r);
                }

                continue;
            }

            switch (r)
            {
                case '"':
                case '\'':
                    inQuote = true;
                    quote = r;
                    break;
                case ' ':
                case '\t':
                    if (cur.Length > 0)
                    {
                        output.Add(cur.ToString());
                        cur.Clear();
                    }

                    break;
                default:
                    cur.Append(r);
                    break;
            }
        }

        if (cur.Length > 0)
        {
            output.Add(cur.ToString());
        }

        return output;
    }
}

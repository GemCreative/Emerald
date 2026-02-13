namespace mycoolapp;

internal static class EntryPoint
{
    private static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            var shellResult = EmeraldShell.Run();
            if (!shellResult.Success)
            {
                Console.Error.WriteLine(shellResult.Message);
                return 1;
            }

            return 0;
        }

        var parsed = CliParser.Parse(args);
        if (!parsed.Success)
        {
            foreach (var line in parsed.Messages)
            {
                Console.Error.WriteLine(line);
            }

            return 1;
        }

        var options = parsed.Options!;
        var result = options.Command switch
        {
            EmeraldCommand.Build => EmeraldCompiler.Build(options),
            EmeraldCommand.Run => EmeraldVm.Run(options),
            EmeraldCommand.Shell => EmeraldShell.Run(),
            EmeraldCommand.Touch => EmeraldShell.TouchCommand(options.Path),
            EmeraldCommand.Carve => EmeraldShell.CarveCommand(options.Path),
            EmeraldCommand.Shine => EmeraldShell.ShineCommand(options.Path),
            _ => CommandResult.Fail("Unknown command."),
        };

        if (!result.Success)
        {
            Console.Error.WriteLine(result.Message);
            return 1;
        }

        if (!string.IsNullOrWhiteSpace(result.Message))
        {
            Console.WriteLine(result.Message);
        }

        return 0;
    }
}

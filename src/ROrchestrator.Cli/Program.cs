namespace ROrchestrator.Cli;

public static class Program
{
    public static int Main(string[] args)
    {
        return ROrchestratorCliApp.Run(args, Console.Out, Console.Error);
    }
}


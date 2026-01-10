namespace Rockestra.Cli;

public static class Program
{
    public static int Main(string[] args)
    {
        return RockestraCliApp.Run(args, Console.Out, Console.Error);
    }
}



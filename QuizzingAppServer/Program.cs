using System;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace QuizzingAppServer
{
  public class Program
  {
    public const int TIMESTAMP_INTERVAL_SEC = 30;
    public const string MONGO_DB_DBN = "quiz";
    public const int BROADCAST_TRANSMIT_INTERVAL_MS = 250;
    public const int CLOSE_SOCKET_TIMEOUT_MS = 2500;

    public static IniFile GlobalConfiguration = new IniFile("Settings.ini");

    public static async System.Threading.Tasks.Task Main(string[] args)
    {

      //var startupProceed = await Licensing.CanStartup();
      var startupProceed = true;

      if (startupProceed)
      {
        Host.CreateDefaultBuilder(args)
        .ConfigureWebHostDefaults(webBuilder =>
        {
          webBuilder.UseUrls(new string[] { $"http://*:8080/" });
          webBuilder.UseStartup<Startup>();
        })
        .Build()
        .Run();
      }
    }

    // should use a logger but hey, it's a demo and it's free
    public static void ReportException(Exception ex, [CallerMemberName] string location = "(Caller name not set)")
    {
      Console.WriteLine($"\n{location}:\n  Exception {ex.GetType().Name}: {ex.Message}");
      if (ex.InnerException != null) Console.WriteLine($"  Inner Exception {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
    }
  }
}

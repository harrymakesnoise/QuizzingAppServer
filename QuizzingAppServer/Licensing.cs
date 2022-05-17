using System;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using AesEverywhere;
using Newtonsoft.Json.Linq;

namespace QuizzingAppServer
{
  public class Licensing
  {
    private static readonly HttpClient WebClient = new HttpClient();

    public static async Task<bool> CanStartup()
    {
      var startupProceed = true;

      if (startupProceed && Program.GlobalConfiguration.GetValue("license", "key") == "")
      {
        Console.WriteLine("No license key found.");
        startupProceed = false;
      }

      if (startupProceed && Program.GlobalConfiguration.GetValue("license", "holder") == "")
      {
        Console.WriteLine("No license holder found.");
        startupProceed = false;
      }

      if (startupProceed && Program.GlobalConfiguration.GetValue("license", "token") == "")
      {
        Console.WriteLine("No license token found.");
        startupProceed = false;
      }

      if (startupProceed)
      {
        var compareTime = GetNetworkTime();
        if (compareTime.Year == 1900)
        {
          compareTime = DateTime.Now;
        }

        AES256 tokenEnc = new AES256();
        //var lastOnlineDTStr = tokenEnc.Decrypt(Program.GlobalConfiguration.GetValue("license", "token"), "D4T3T1M3");
        var lastOnlineDTStr = "2020-02-22";
        var lastOnlineDT = DateTime.Parse(lastOnlineDTStr);
        var requiredOnline = false;

        if (lastOnlineDT.AddDays(14) < compareTime)
        {
          requiredOnline = true;
        }

        if (requiredOnline)
        {
          var licenseStr = "{\"l\": \"" + Program.GlobalConfiguration.GetValue("license", "key") + "\", \"h\": \"" + Program.GlobalConfiguration.GetValue("license", "holder") + "\"}";

          var data = new StringContent(licenseStr, Encoding.UTF8, "application/json");

          var url = "https://quizzing.app/license-check";

          var response = await WebClient.PostAsync(url, data);

          string result = response.Content.ReadAsStringAsync().Result;
          Console.WriteLine(result);
        }
        startupProceed = false;
      }
      return startupProceed;
    }
    public static DateTime GetNetworkTime()
    {
      const string ntpServer = "pool.ntp.org";
      var ntpData = new byte[48];
      ntpData[0] = 0x1B; //LeapIndicator = 0 (no warning), VersionNum = 3 (IPv4 only), Mode = 3 (Client Mode)

      var addresses = Dns.GetHostEntry(ntpServer).AddressList;
      var ipEndPoint = new IPEndPoint(addresses[0], 123);
      var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

      socket.ReceiveTimeout = 3000;
      socket.Connect(ipEndPoint);
      socket.Send(ntpData);
      socket.Receive(ntpData);
      socket.Close();

      ulong intPart = (ulong)ntpData[40] << 24 | (ulong)ntpData[41] << 16 | (ulong)ntpData[42] << 8 | (ulong)ntpData[43];
      ulong fractPart = (ulong)ntpData[44] << 24 | (ulong)ntpData[45] << 16 | (ulong)ntpData[46] << 8 | (ulong)ntpData[47];

      var milliseconds = (intPart * 1000) + ((fractPart * 1000) / 0x100000000L);
      var networkDateTime = (new DateTime(1900, 1, 1)).AddMilliseconds((long)milliseconds);

      return networkDateTime;
    }
  }
}

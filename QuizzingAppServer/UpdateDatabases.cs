using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Newtonsoft.Json.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using System.IO;

namespace QuizzingAppServer
{
  internal class UpdateDatabases : IHostedService, IDisposable
  {
    private Timer timer;
    private IMongoDatabase database = new MongoClient(Program.MONGO_DB_SVR).GetDatabase(Program.MONGO_DB_DBN);
    private ConcurrentDictionary<long, ConcurrentDictionary<long, JObject>> OldGameData = new ConcurrentDictionary<long, ConcurrentDictionary<long, JObject>>();
    public string LastQuery = "";
    private int TryCount = 0;

    public Task StartAsync(CancellationToken cancellationToken)
    {
      var interval = TimeSpan.FromSeconds(Program.TIMESTAMP_INTERVAL_SEC);
      timer = new Timer(UpdateDBs, null, TimeSpan.Zero, interval);
      return Task.CompletedTask;
    }

    private async Task<bool> GetCurrentGameData()
    {
      var allSitesData = WebSocketMiddleware.CurrentGameData;
      var count = 0;
      var gameCount = 0;
      foreach(var site in allSitesData)
      {
        count++;
        Console.WriteLine("UpdateDB: Loading sites " + count + " of " + allSitesData.Count);
        var siteid = site.Key;
        var siteGameCount = 0;
        foreach (var game in site.Value)
        {
          gameCount++;
          siteGameCount++;
          Console.WriteLine("UpdateDB: Loading games " + siteGameCount + " of " + site.Value.Count);
          var gameid = game.Key;
          var nowDt = DateTime.Now;
          var DateTimeStr = nowDt.ToString("yyyyMMddTHH:mm:ssZ");
          var cdata = game.Value;

          cdata["gameid"] = gameid;
          cdata["siteid"] = siteid;
          cdata["datetime"] = DateTimeStr;

          if(cdata.ContainsKey("expirytime"))
          {
            var now = DateTime.Now;
            var expiryTime = DateTime.Parse(cdata["expirytime"].Value<string>());
            if (expiryTime < now)
            {
              await WebSocketMiddleware.RemoveGameData(siteid, gameid);
            }
          }

          JObject packet = new JObject(
                          new JProperty("collectionname", "current_game_data"),
                          new JProperty("action", "replace"),
                          new JProperty("data", cdata),
                          new JProperty("packetfilter", new JObject(
                              new JProperty("siteid", siteid),
                              new JProperty("gameid", gameid)))
                        );
          WebSocketMiddleware.dbActions.Insert(0, packet);
        }
      }
      Console.WriteLine("UpdateDB: Total - " + gameCount + " game(s) across " + count + " site(s) to update.");
      return true;
    }

    private async void UpdateDBs(object state)
    {
      if (WebSocketMiddleware.GarbageCollection())
      {
        try
        {
          await GetCurrentGameData();

          Console.WriteLine("UpdateDB: " + WebSocketMiddleware.dbActions.Count + " item(s) to process");
          if (WebSocketMiddleware.dbActions.Count > 0)
          {
            var ogCount = WebSocketMiddleware.dbActions.Count;
            for (int i = WebSocketMiddleware.dbActions.Count - 1; i >= 0; i--)
            {
              Console.WriteLine("UpdateDB: Processing " + (i + 1) + " of " + ogCount);
              var packet = WebSocketMiddleware.dbActions[i];
              var collection = database.GetCollection<BsonDocument>(packet["collectionname"].ToString());
              var packetFilter = packet["packetfilter"] != null ? packet["packetfilter"].ToString() : null;
              var action = packet["action"].ToString();
              LastQuery = packet.ToString();

              if (action == "delete")
              {
                if (packetFilter != null)
                {
                  //Console.WriteLine("UpdateDBs: Delete packets - " + packetFilter);
                  await collection.DeleteManyAsync(packetFilter);
                }
                else
                {
                  Console.WriteLine("Failed to delete packets - no filter detected");
                }
              }
              else if (action == "insert")
              {
                //Console.WriteLine("UpdateDBs: Insert packet - " + packet["data"]);
                await collection.InsertOneAsync(BsonDocument.Parse(packet["data"].ToString()));
              }
              else if (action == "replace")
              {
                if (packetFilter != null)
                {
                  //Console.WriteLine("UpdateDBs: Update packet - " + packetFilter + " / " + packet["data"]);
                  await collection.ReplaceOneAsync(
                      filter: BsonDocument.Parse(packetFilter.ToString()),
                      options: new ReplaceOptions { IsUpsert = true },
                      replacement: BsonDocument.Parse(packet["data"].ToString()));
                }
                else
                {
                  Console.WriteLine("Failed to update " + WebSocketMiddleware.dbActions[i] + " - No filter detected");
                }
              }
              else if (action == "update")
              {
                if(packetFilter != null)
                {
                  await collection.UpdateOneAsync(
                   filter: BsonDocument.Parse(packetFilter.ToString()),
                   Builders<BsonDocument>.Update.Set(packet["data"]["field"].ToString(), packet["data"]["value"].ToString()));
                }
              }
              WebSocketMiddleware.dbActions[i].Remove();
            }
          }
          Console.WriteLine("UpdateDB: Processing finished. " + WebSocketMiddleware.dbActions.Count + " item(s) left in queue");
        }
        catch (Exception e)
        {
          if (TryCount == 0)
          {
            TryCount++;
            database = new MongoClient(Program.MONGO_DB_SVR).GetDatabase(Program.MONGO_DB_DBN);
            UpdateDBs(state);
          }
          else
          {
            TryCount = 0;
            await File.WriteAllTextAsync("DBFails.txt", LastQuery);
            Program.ReportException(e);
          }
        }
      }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
      timer?.Change(Timeout.Infinite, 0);
      return Task.CompletedTask;
    }

    public void Dispose()
    {
      timer?.Dispose();
    }
  }
}
